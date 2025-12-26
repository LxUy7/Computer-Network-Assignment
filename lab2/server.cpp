// g++ server.cpp -o server.exe -lws2_32 -std=c++11

/*第一阶段：握手 (Handshake) —— 确认双方在线
代码位置：Client handshake() 和 Server handshake()
Client: 发送一个带 SYN 标志的包。进入 while(true) 循环等待。
Server: 收到 SYN，记录 Client 的序列号，回发 SYN + ACK。
Client: 收到 SYN + ACK，跳出循环，回发最后一个 ACK。
关键点：如果 Client 等不到回复（超时），会重发 SYN（代码里有个 1000ms 的判断）。

第二阶段：核心传输循环 (The Main Loop) —— 发送方的引擎
代码位置：Client send_file() 中的 while (base < buffer.size()) 循环
这是整个程序的心脏。Client 并不是发一个等一个，而是流水线发送。
谁在控制发送速度？
变量 effective_window = min(cwnd, rwnd)。
含义：我能发的包数量 = 拥塞窗口（我想发的）和 接收窗口（对方能收的）取最小值。
怎么发？
只要 next_seq（下一个待发包索引）在窗口范围内，就调用 send_udp() 发出去，并记录 send_time（用来算是否超时）。
怎么收 ACK？
Socket 设置了非阻塞模式 (FIONBIO)。
recvfrom 不会卡死。如果有 ACK 就处理，没 ACK 就继续往下跑（去检查超时）。

第三阶段：拥塞控制 (Reno) —— 代码最复杂的地方
代码位置：Client send_file() 收到 ACK 后的 if-else 块
怎么体现慢启动和拥塞避免？
三种状态的跳转：
    慢启动 (Slow Start):
    条件：cwnd < ssthresh
    行为：收到新 ACK，cwnd += 1.0。
    效果：窗口指数增长（1, 2, 4, 8...），飞快提速。
    拥塞避免 (Congestion Avoidance):
    条件：cwnd >= ssthresh
    行为：收到新 ACK，cwnd += 1.0 / cwnd。
    效果：窗口线性增长，慢慢试探网络上限。
    丢包处理 (核心加分项):
        情况 A：收到 3 个重复 ACK (快重传)
        现象：ack_val <= base_seq_num，导致 dup_ack_count == 3。
        动作：
        立即重传 buffer[base]（不用等超时）。
        快恢复：ssthresh 减半，cwnd = ssthresh + 3。
        解释：网络没断，只是丢了一个包，所以温和地减速。
        情况 B：超时 (Timeout)
        现象：time_since_send > INITIAL_RTO。
        动作：
        ssthresh 减半。
        cwnd 直接重置为 1。
        解释：网络堵死了，必须从头开始慢启动。

第四阶段：接收端与 SACK 
代码位置：Server receive_file()
接收逻辑：
如果收到 seq == expected_seq（正好是我要的）：直接写文件。
如果收到 seq > expected_seq（乱序，后面的先到了）：存入 Map 缓冲区 (out_of_order_buffer)。
SACK 是什么？
Server 在回发 ACK 时，会查一下 Map 缓冲区里有哪些包，把第一个连续块的范围（sack_start 到 sack_end）填进包头。
Client 收到后：遍历自己的发送列表，把这些包标记为 is_acked = true。下次重传时，就不会傻傻地重传这些已经到达的包了。*/


#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <WinSock2.h>
#include <WS2tcpip.h>
#include <string.h>
#include <vector>
#include <map>          // 用于接收缓冲区
#include <thread>
#include <chrono>       // 用于计时
#include <fstream>
#include <random>       // 用于随机丢包

using namespace std;

#pragma comment(lib, "ws2_32.lib")

// 配置参数
#define SERVER_PORT 8090
#define MSS 1024        // 最大分段大小 (数据部分)
#define RCV_WND 20      // 接收窗口大小 (单位: 包数量)
#define LOSS_RATE 3   // 模拟接收端丢包率 0-20

// 标志位定义
#define SYN 0x01 // 建立连接 标识连接建立请求
#define ACK 0x02 // 确认
#define FIN 0x04 // 结束连接
#define DATA 0x08 // 数据包

SOCKET server_socket;
sockaddr_in client_addr;
int addr_len = sizeof(client_addr);
uint32_t expected_seq = 0; // 期待收到的下一个序列号

// 协议头部结构
#pragma pack(1)
struct Header {
    uint32_t seq_num;
    uint32_t ack_num;
    uint16_t length;      // 数据部分长度
    uint16_t checksum;
    uint8_t  flags;
    uint8_t  window_size; // 接收窗口通告
    uint32_t sack_start;
    uint32_t sack_end;
};

// 完整的消息结构
struct Message {
    Header head;
    char data[MSS]; // 数据载荷

    Message() {
        memset(&head, 0, sizeof(Header));
        memset(data, 0, MSS);
    }

    // 设置标志位
    void set_flag(uint8_t flag) { head.flags |= flag; }
    // 检查标志位
    bool has_flag(uint8_t flag) { return (head.flags & flag) != 0; }

    // 计算校验和 (发送前调用)
    /*把所有数据当成数字加起来，取反作为校验码。
    接收方收到后，再把所有数据（含校验码）加一遍，如果结果是“全 1”，说明没出错。*/
    void set_checksum() {
        head.checksum = 0;
        uint32_t sum = 0;
        uint16_t* p = (uint16_t*)this;
        // 计算 Header + Data 的总长度 
        int size_bytes = sizeof(Header) + head.length;
        
        // 处理偶数字节
        for (int i = 0; i < size_bytes / 2; i++) {
            sum += *p++;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        // 处理奇数尾部
        if (size_bytes % 2 != 0) {
            uint16_t tmp = (*(uint8_t*)p); // 填充0
            sum += tmp;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        head.checksum = ~(sum & 0xffff); // 取反
    }

    // 验证校验和 (接收后调用)
    bool check_checksum() {
        uint32_t sum = 0;
        uint16_t* p = (uint16_t*)this;
        int size_bytes = sizeof(Header) + head.length;

        for (int i = 0; i < size_bytes / 2; i++) {
            sum += *p++;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        if (size_bytes % 2 != 0) {
            uint16_t tmp = (*(uint8_t*)p);
            sum += tmp;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        return (sum & 0xffff) == 0xffff; // 结果应为全1 (0xFFFF)
    }

    // 调试打印
    void print(const char* tag) {
        printf("[%s] Seq:%u Ack:%u Len:%u Flg:", tag, head.seq_num, head.ack_num, head.length);
        if (has_flag(SYN)) printf("SYN ");
        if (has_flag(FIN)) printf("FIN ");
        if (has_flag(ACK)) printf("ACK ");
        if (has_flag(DATA)) printf("DATA ");
        if (head.sack_start != 0) printf(" SACK[%u-%u]", head.sack_start, head.sack_end);
        printf("\n");
    }
};
#pragma pack() // 恢复对齐


// 模拟丢包
bool is_packet_lost() {
    if (LOSS_RATE == 0) return false;
    static std::default_random_engine e(time(0));
    static std::uniform_int_distribution<int> u(0, 99);
    return u(e) < LOSS_RATE; // 如果随机数小于丢包率，则返回 true
}

// 初始化 Socket
bool init_socket() {
    server_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (server_socket == INVALID_SOCKET) return false;

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); // 监听所有网卡IP
    addr.sin_port = htons(SERVER_PORT);

    if (bind(server_socket, (SOCKADDR*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        std::cout << "Bind failed: " << WSAGetLastError() << endl;
        return false;
    }
    
    // 增大内核接收缓冲区大小，防止大量突发数据导致系统层面的丢包
    int rcv_buf_size = 1024 * 1024; // 1MB
    setsockopt(server_socket, SOL_SOCKET, SO_RCVBUF, (const char*)&rcv_buf_size, sizeof(int));

    std::cout << "Server started at port " << SERVER_PORT << endl;
    return true;
}

// 发送消息
void send_msg(Message& msg) {
    msg.set_checksum();
    // 模拟ACK丢包
    if (is_packet_lost()) return; 
    sendto(server_socket, (char*)&msg, sizeof(Header) + msg.head.length, 0, (SOCKADDR*)&client_addr, addr_len);
}


// 三次握手（被动打开）
bool handshake() {
    Message recv_msg, tx_msg;
    std::cout << "等待连接..." << endl;

    // 等待client发送SYN
    while (true) {
        int ret = recvfrom(server_socket, (char*)&recv_msg, sizeof(recv_msg), 0, (SOCKADDR*)&client_addr, &addr_len);
        if (ret > 0 && recv_msg.check_checksum() && recv_msg.has_flag(SYN)) {
            recv_msg.print("RECV Handshake");
            // 记录对方的 seq序列号，下一次期望收到的 seq 为 SYN seq + 1
            expected_seq = recv_msg.head.seq_num + 1; // 消耗一个序号
            break;
        }
    }

    // 发送syn和ack
    tx_msg.set_flag(SYN);
    tx_msg.set_flag(ACK);
    tx_msg.head.seq_num = 0;      // Server 初始 seq序列号
    tx_msg.head.ack_num = expected_seq;
    tx_msg.print("SEND Handshake");
    send_msg(tx_msg);

    // 等待client发送ack
    while (true) {
        int ret = recvfrom(server_socket, (char*)&recv_msg, sizeof(recv_msg), 0, (SOCKADDR*)&client_addr, &addr_len);
        if (ret > 0 && recv_msg.check_checksum() && recv_msg.has_flag(ACK)) {
            recv_msg.print("RECV Handshake");
            std::cout << "已建立连接" << endl;
            return true;
        }
    }
    return false;
}

// 数据传输 (接收窗口 + SACK + 统计)
void receive_file() {
    Message recv_msg, ack_msg;
    // 乱序缓冲区：使用 map 自动按 Seq 排序，Key=Seq, Value=Message
    std::map<uint32_t, Message> out_of_order_buffer; // 接收缓冲区 (SACK缓冲)
    ofstream out_file;
    long total_bytes = 0;
    
    // 打开文件用于保存
    out_file.open("received_file.jpg", ios::out | ios::binary);
    if (!out_file) {
        perror("Cannot open file");
        return;
    }

    std::cout << "Start Receiving Data..." << endl;
    auto start_time = std::chrono::high_resolution_clock::now();

    while (true) {
        // 阻塞接收数据
        int ret = recvfrom(server_socket, (char*)&recv_msg, sizeof(recv_msg), 0, (SOCKADDR*)&client_addr, &addr_len);
        if (ret <= 0) continue;

        // 模拟丢包
        if (is_packet_lost()) {
            // std::cout << "Packet Loss Simulated: Seq " << recv_msg.head.seq_num << endl;
            continue;
        }

        // 校验和检查完整性
        if (!recv_msg.check_checksum()) {
            std::cout << "Checksum Error!" << endl;
            continue;
        }

        // 处理 FIN (结束连接)
        if (recv_msg.has_flag(FIN)) {
            std::cout << "Received FIN. End of transmission." << endl;
            expected_seq++; // FIN 消耗一个序号
            break;
        }

        // 数据处理逻辑
        uint32_t seq = recv_msg.head.seq_num;
        uint32_t len = recv_msg.head.length;

        // 忽略纯 ACK 包 (Server在此处主要是单向接收数据)
        if (len == 0 && !recv_msg.has_flag(SYN) && !recv_msg.has_flag(FIN)) {
            // 纯ACK包
            continue;
        }

        // A. 收到期望的有序包
        if (seq == expected_seq) {
            // 写入文件
            out_file.write(recv_msg.data, len);
            total_bytes += len;
            expected_seq += len;

            // 检查缓冲区中是否有连续的数据
            while (!out_of_order_buffer.empty()) {
                auto it = out_of_order_buffer.begin();
                if (it->first == expected_seq) {
                    // 缓冲区头部正好接上
                    out_file.write(it->second.data, it->second.head.length);
                    total_bytes += it->second.head.length;
                    expected_seq += it->second.head.length;
                    out_of_order_buffer.erase(it);// 从缓存移除

                } else {
                    break; 
                }
            }
        }

        // B.收到乱序包 (Seq > Expected)，放入MAP缓冲区并发送 SACK
        else if (seq > expected_seq) {
            // 防止缓冲区无限膨胀，仅在缓冲区未满时存储
            if (out_of_order_buffer.size() < RCV_WND) {
                out_of_order_buffer[seq] = recv_msg; // 存入Map自动排序
            }
        }
    
        // C: 收到旧包 (Duplicate)，seq < expected_seq，直接忽略数据，但需重发ACK
        // 收到旧包 (Seq < Expected)，重发当前 expected_seq 的 ACK 
        ack_msg.head.seq_num = 0; // Server端单向传输
        ack_msg.head.ack_num = expected_seq; // 累计确认
        ack_msg.set_flag(ACK);
        // 流量控制：通告剩余接收窗口大小
        ack_msg.head.window_size = (uint8_t)(RCV_WND - out_of_order_buffer.size()); // 流量控制
        
        // 填充 SACK,告诉客户端收到了哪个未来的块
        if (!out_of_order_buffer.empty()) {
            // 只通告缓冲区里的第一个连续块，把第一个连续块的范围（sack_start 到 sack_end）填进包头。
            ack_msg.head.sack_start = out_of_order_buffer.begin()->first;
            ack_msg.head.sack_end = ack_msg.head.sack_start + out_of_order_buffer.begin()->second.head.length;
        } else {
            ack_msg.head.sack_start = 0;
            ack_msg.head.sack_end = 0;
        }

        send_msg(ack_msg);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;
    out_file.close();

    // --- 输出统计信息 ---
    double time_s = diff.count();
    double speed_kbps = (total_bytes * 8.0) / 1000.0 / time_s;
    double speed_MBs = (total_bytes / 1024.0 / 1024.0) / time_s;

    std::cout << "\n========== 报告 ==========" << endl;
    std::cout << "总字节:" << total_bytes << " Bytes" << endl;
    std::cout << "传输用时:" << time_s << " s" << endl;
    std::cout << "平均吞吐率:" << speed_kbps << " Kbps (" << speed_MBs << " MB/s)" << endl;
    std::cout << "丢包率:" << LOSS_RATE << "%" << endl;
    std::cout << "=========================================\n" << endl;
}

// 四次挥手 (被动关闭)
void teardown() {
    Message recv_msg, tx_msg;
    
    // 已收到 FIN，现在处于 CLOSE_WAIT
    // 发送 ACK 确认 FIN
    tx_msg.set_flag(ACK);
    tx_msg.head.ack_num = expected_seq;
    send_msg(tx_msg);
    
    // 发送我方 FIN
    tx_msg.head.flags = 0; // 清空旧标志
    tx_msg.set_flag(FIN);
    tx_msg.set_flag(ACK);
    tx_msg.head.seq_num = 100; 
    tx_msg.head.ack_num = expected_seq;
    send_msg(tx_msg);
    std::cout << "SEND FIN+ACK" << endl;

    // 等待最后的 ACK
    // 设置 socket 超时，防止最后一步死锁
    struct timeval tv;
    tv.tv_sec = 2; tv.tv_usec = 0;
    setsockopt(server_socket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    int ret = recvfrom(server_socket, (char*)&recv_msg, sizeof(recv_msg), 0, (SOCKADDR*)&client_addr, &addr_len);
    if (ret > 0 && recv_msg.has_flag(ACK)) {
        std::cout << "已收到最后的ACK, 关闭连接" << endl;
    } else {
        std::cout << "等待ACK超时,仍关闭" << endl;
    }
    
    closesocket(server_socket);// 彻底释放资源
}

int main() {
    // Windows Socket 初始化
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cout << "WSA启动失败" << endl;
        return -1;
    }

    if (!init_socket()) {
        return -1;
    }

    // 主循环：支持处理完一个连接后，等待下一个连接
    while (true) {
        if (handshake()) {
            receive_file();
            teardown();
        }
        std::cout << "准备新连接" << endl;
        // 重置状态
        expected_seq = 0;
        // 恢复 Socket 为阻塞模式 (因为 teardown 里设置了超时，这里要改回来)
        struct timeval tv;
        tv.tv_sec = 0; tv.tv_usec = 0;
        setsockopt(server_socket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
        if (!init_socket()) break; 
    }

    WSACleanup();
    return 0;
}