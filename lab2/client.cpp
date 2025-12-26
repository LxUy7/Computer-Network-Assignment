// g++ client.cpp -o client.exe -lws2_32 -std=c++11
#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <WinSock2.h>
#include <WS2tcpip.h>
#include <string.h>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iomanip>

using namespace std;
#pragma comment(lib, "ws2_32.lib")

// 配置参数
#define SERVER_PORT 8090
#define CLIENT_PORT 8080
#define MSS 1024        // 最大分段大小 (数据部分)
#define INITIAL_RTO 500 // 初始超时时间 (ms)

// 标志位
#define SYN 0x01 // 同步标志，用于连接建立
#define ACK 0x02 // 确认标志
#define FIN 0x04 // 结束标志，用于断开连接
#define DATA 0x08 // 数据包标志

SOCKET client_socket;
sockaddr_in server_addr;
int server_addr_len = sizeof(server_addr);

// 拥塞控制变量
double cwnd = 1.0;      // 拥塞窗口，单位：MSS
double ssthresh = 32.0; // 慢启动阈值
uint32_t rwnd = 20;     // 接收方通告窗口 ，由对方通过ACK告知
int dup_ack_count = 0;  // 重复ACK计数器，用于触发快重传

// 协议头部
#pragma pack(1) // 设置结构体按1字节对齐，防止内存填充导致网络传输解析错误
struct Header {
    uint32_t seq_num;    // 序列号
    uint32_t ack_num;    // 确认号
    uint16_t length;     // 数据长度
    uint16_t checksum;   // 校验和
    uint8_t  flags;      // 标志位
    uint8_t  window_size;// 接收窗口大小通告

    // SACK选项字段
    uint32_t sack_start; // SACK 起始序列号
    uint32_t sack_end;   // SACK 结束序列号
};

struct Message {
    Header head;
    char data[MSS];

    Message() { memset(this, 0, sizeof(Message)); }

    // 设置标志位
    void set_flag(uint8_t flag) { head.flags |= flag; }
    // 检查标志位
    bool has_flag(uint8_t flag) { return (head.flags & flag) != 0; }

    // 计算并设置校验和 (由发送方调用)
    void set_checksum() {
        head.checksum = 0;
        uint32_t sum = 0;
        uint16_t* p = (uint16_t*)this;
        // 计算 Header + 数据部分的 16位字和
        int size_bytes = sizeof(Header) + head.length;
        for (int i = 0; i < size_bytes / 2; i++) {
            sum += *p++;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        // 处理奇数字节
        if (size_bytes % 2 != 0) {
            uint16_t tmp = (*(uint8_t*)p);
            sum += tmp;
            while (sum >> 16) sum = (sum & 0xffff) + (sum >> 16);
        }
        head.checksum = ~(sum & 0xffff); // 取反
    }

    // 验证校验和 (由接收方调用)
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
        return (sum & 0xffff) == 0xffff; // 结果应为全1
    }

    // 调试打印函数
    void print(const char* tag) {
        printf("[%s] Seq:%u Ack:%u Len:%u Cwnd:%.1f Rwnd:%u\n", 
               tag, head.seq_num, head.ack_num, head.length, cwnd, rwnd);
    }
};
#pragma pack() // 恢复默认对齐

// 发送缓冲区元数据
struct PacketInfo {
    Message msg;
    bool is_acked;      // 是否已被SACK或累积ACK确认
    std::chrono::steady_clock::time_point send_time;// 上次发送时间 (用于计算 RTO)
    bool sent_once;     // 是否已经发送过至少一次
};

// 初始化
bool init_socket() {
    client_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (client_socket == INVALID_SOCKET) return false;

    // 设置 Socket 为非阻塞模式 (Non-blocking Mode)
    // 这样 recvfrom 不会卡死，可以在循环中处理超时逻辑
    u_long mode = 1;
    ioctlsocket(client_socket, FIONBIO, &mode);

    // 绑定本地端口
    sockaddr_in local_addr;
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    local_addr.sin_port = htons(CLIENT_PORT);
    bind(client_socket, (SOCKADDR*)&local_addr, sizeof(local_addr));

    // 设置服务器地址
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    return true;
}

// 辅助函数：发送 UDP 数据包
void send_udp(Message& msg) {
    msg.set_checksum();
    sendto(client_socket, (char*)&msg, sizeof(Header) + msg.head.length, 0, (SOCKADDR*)&server_addr, server_addr_len);
}

// ================= 三次握手 =================
// 握手与挥手
bool handshake() {
    std::cout << "Starting 3-way Handshake..." << endl;
    Message syn, ack_msg;
    
    // 发送SYN
    syn.set_flag(SYN);
    syn.head.seq_num = 0;
    send_udp(syn);
    
    auto start = std::chrono::steady_clock::now();
    
    // 等待 SYN+ACK
    while (true) {
        // 超时1000ms重传 SYN
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() > 1000) {
            std::cout << "Handshake timeout, retrying..." << endl;
            send_udp(syn);
            start = std::chrono::steady_clock::now();
        }

        int ret = recvfrom(client_socket, (char*)&ack_msg, sizeof(ack_msg), 0, (SOCKADDR*)&server_addr, &server_addr_len);
        // 收到 SYN+ACK ?
        if (ret > 0 && ack_msg.check_checksum() && ack_msg.has_flag(SYN) && ack_msg.has_flag(ACK)) {
            break; // 握手第二步成功
        }
    }

    // 发送最后一个ACK 完成握手
    Message last_ack;
    last_ack.set_flag(ACK);
    last_ack.head.seq_num = 1;
    last_ack.head.ack_num = ack_msg.head.seq_num + 1;
    send_udp(last_ack);
    
    std::cout << "握手成功" << endl;
    return true;
}

// ================= 四次挥手 =================
void teardown() {
    std::cout << "开始四次挥手..." << endl;
    Message fin, msg;
    
    // 发送FIN（主动关闭
    fin.set_flag(FIN);
    fin.set_flag(ACK); // 通常携带ACK
    send_udp(fin);

    // 等待对方的 ACK 和 FIN，循环等待直到收到 FIN
    auto start = std::chrono::steady_clock::now();
    bool received_fin = false;

    while (!received_fin) {
        // 3秒总超时，防止死锁
         if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() > 3000) {
            std::cout << "超时, 强制关闭" << endl;
            break;
        }

        int ret = recvfrom(client_socket, (char*)&msg, sizeof(msg), 0, (SOCKADDR*)&server_addr, &server_addr_len);
        if (ret > 0 && msg.check_checksum()) {
            if (msg.has_flag(FIN)) {
                received_fin = true;
                // 收到服务器的 FIN，发送最后的 ACK
                Message last_ack;
                last_ack.set_flag(ACK);
                send_udp(last_ack);
                std::cout << "收到FIN, 发送ACK, 关闭" << endl;
            }
        }
    }
}

// 传输 (Reno + SACK)
bool send_file(const char* filename) {
    // 读取文件到内存
    ifstream file(filename, ios::binary | ios::ate);
    if (!file) { cerr << "File not found!" << endl; return false; }
    long file_size = file.tellg();
    file.seekg(0, ios::beg);

    // 将文件切分为数据包并存入 buffer
    vector<PacketInfo> buffer;
    long bytes_read = 0;
    uint32_t seq_counter = 1; // 0 用作了握手，数据从1开始 (相对Seq)

    while (bytes_read < file_size) {
        PacketInfo pkt;
        pkt.msg.head.seq_num = (uint32_t)bytes_read + 1; // 序列号 = 字节偏移量 + 1
        long to_read = min((long)MSS, file_size - bytes_read);
        file.read(pkt.msg.data, to_read);
        pkt.msg.head.length = (uint16_t)to_read;
        pkt.msg.set_flag(DATA);
        pkt.is_acked = false;
        pkt.sent_once = false;
        buffer.push_back(pkt);
        bytes_read += to_read;
    }
    file.close();
    
    std::cout << "文件已加载,总数据包 " << buffer.size() << " (" << file_size << " bytes)" << endl;

    // 传输状态变量
    int base = 0;      // 发送窗口基准索引 (最早未确认的包)
    int next_seq = 0;  // 下一个待发送的包索引 （窗口右沿
    cwnd = 1.0;        // 初始化拥塞窗口 (慢启动)
    ssthresh = 32.0;   // 慢启动阈值
    dup_ack_count = 0; // 重复ACK计数
    
    auto t_start = std::chrono::high_resolution_clock::now();
    Message ack_pkt;

    // 主循环（直到所有包都被确认
    while (base < buffer.size()) {
        auto now = std::chrono::steady_clock::now();

        // 发送逻辑
        // 有效窗口 = min(cwnd, rwnd)拥塞窗口（我想发的), 接收方窗口
        int effective_window = min((int)cwnd, (int)rwnd);
        
        // 在窗口范围内发送未发送的数据包
        while (next_seq < buffer.size() && next_seq < base + effective_window) {
            if (!buffer[next_seq].is_acked) {
                // 只要 next_seq（下一个待发包索引）在窗口范围内，就调用 send_udp() 发出去，并记录 send_time（用来算是否超时）。
                if (!buffer[next_seq].sent_once) {
                    send_udp(buffer[next_seq].msg);
                    buffer[next_seq].send_time = std::chrono::steady_clock::now();
                    buffer[next_seq].sent_once = true;
                }
            }
            next_seq++;
        }

        // 接收 ACK 逻辑
        //Socket 设置了非阻塞模式 (FIONBIO)。
        //recvfrom 不会卡死。如果有 ACK 就处理，没 ACK 就继续往下跑（去检查超时）。
        int ret = recvfrom(client_socket, (char*)&ack_pkt, sizeof(ack_pkt), 0, (SOCKADDR*)&server_addr, &server_addr_len);
        
        if (ret > 0 && ack_pkt.check_checksum() && ack_pkt.has_flag(ACK)) {
            uint32_t ack_val = ack_pkt.head.ack_num;
            // 更新接收方窗口
             rwnd = ack_pkt.head.window_size;
             if (rwnd == 0) rwnd = 1; // 避免死锁，保持至少能发1个包探测

            // SACK 处理 (标记乱序收到的包)
            if (ack_pkt.head.sack_start > 0 && ack_pkt.head.sack_end > 0) {
                for (int i = base; i < next_seq; i++) {
                    uint32_t pkt_seq = buffer[i].msg.head.seq_num;
                    // 遍历发送列表，把这些包标记为 is_acked = true
                    // 下次重传时，就不会重传这些已经到达的包
                    if (pkt_seq >= ack_pkt.head.sack_start && pkt_seq < ack_pkt.head.sack_end) {
                        buffer[i].is_acked = true; 
                    }
                }
            }

            // 累计确认处理，需要找到 Buffer 中对应的索引。
            uint32_t base_seq_num = buffer[base].msg.head.seq_num;

            if (ack_val > base_seq_num) {
                // New ACK (收到了新数据的确认)，更新 Base (可能跨越多个包)
                while (base < buffer.size() && buffer[base].msg.head.seq_num < ack_val) {
                    buffer[base].is_acked = true;
                    base++;
                }

                // Reno: 状态更新（三种）
                dup_ack_count = 0;// 重置重复ACK计数
                if (cwnd < ssthresh) {
                    // A. 慢启动（窗口指数增长
                    cwnd += 1.0;
                } else {
                    // B. 拥塞避免 (线性增长),慢慢试探网络上限
                    cwnd += 1.0 / cwnd;
                }

            } else {
                // 丢包处理
                // Duplicate ACK (收到了旧的确认，意味着可能丢包)
                dup_ack_count++;
                
                if (dup_ack_count == 3) {
                    // 快速重传
                    // 收到3个重复ACK，认为 Base 包丢失，立即重传，不等待超时
                    // ack_val <= base_seq_num，导致 dup_ack_count == 3
                    std::cout << "[Reno] 快速重传已启动, 重发数据包索引 " << base << endl;
                    send_udp(buffer[base].msg); //立即重传，不用等待超时
                    buffer[base].send_time = std::chrono::steady_clock::now(); // 重置计时器
                    
                    // 快速恢复
                    ssthresh = max(cwnd / 2, 2.0); // 阈值减半
                    // 网络没断，只是丢了一个包，所以温和减速。
                    cwnd = ssthresh + 3; // 窗口设置为 阈值 + 3 (因为收到了3个重复ACK，说明有3个包离开了网络)
                }
            }
        }

        // 超时处理, 只检查窗口基准包 (base) 是否超时
        if (base < buffer.size()) {
            auto time_since_send = std::chrono::duration_cast<std::chrono::milliseconds>(now - buffer[base].send_time).count();
            if (buffer[base].sent_once && time_since_send > INITIAL_RTO) {
                std::cout << "[Timeout] 数据包 " << base << " 已超时 RTO=" << INITIAL_RTO << "ms" << endl;
                
                // 超时逻辑，Reno算法处理
                ssthresh = max(cwnd / 2, 2.0); // 阈值减半
                cwnd = 1.0;// 拥塞窗口充重置 1（进入慢启动），网络堵死了，必须从头开始慢启动
                dup_ack_count = 0;

                // 重传 Base 包
                send_udp(buffer[base].msg);
                buffer[base].send_time = std::chrono::steady_clock::now();
                
                // 重置 next_seq
                // 因为上面有 SACK 标记，下次循环只会重传未被 SACK 的包
                next_seq = base + 1; 
            }
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = t_end - t_start;
    
    // 统计报告
    double time_s = diff.count();
    double tput = (file_size * 8.0) / 1000.0 / time_s; // Kbps

    std::cout << "\n========== 完成传输 ==========" << endl;
    std::cout << "文件大小: " << file_size << " bytes" << endl;
    std::cout << "传输用时: " << time_s << " s" << endl;
    std::cout << "平均吞吐率: " << tput << " Kbps (" << (file_size/1024.0/1024.0)/time_s << " MB/s)" << endl;
    std::cout << "========================================" << endl;

    return true;
}

int main() {
    // 启动 Winsock
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) return -1;

    if (!init_socket()) {
        cerr << "Socket初始化失败" << endl;
        return -1;
    }

    // 交互界面
    while (true) {
        std::cout << "\n[1] 发送文件\n[2] 退出\n选择: ";
        int choice;
        cin >> choice;
        
        if (choice == 1) {
            char filename[256];
            std::cout << "输入文件: ";
            cin >> filename;
            
            // 执行流程：握手 -> 传文件 -> 挥手
            if (handshake()) {
                send_file(filename);
                teardown();
            } else {
                cerr << "握手失败" << endl;
            }
        } else {
            break;
        }
    }

    closesocket(client_socket);
    WSACleanup();
    return 0;
}