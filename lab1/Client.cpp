#include <iostream>
#include <thread>
#include <winsock2.h>
#include <ctime>
#include <cstring>
#pragma comment(lib, "ws2_32.lib")

#pragma pack(push, 1)
struct Message {
    int senderID;
    int targetID;       
    char name[50];
    char content[512];
    char time[50];
};
#pragma pack(pop)

const int PORT = 8080;
const char* SERVER_IP = "127.0.0.1";

void receiveMessages(SOCKET clientSocket) {
    Message msg;

    while (true) {
        int bytesRead = recv(clientSocket, (char*)&msg, sizeof(msg), 0);
        if (bytesRead <= 0) {
            std::cout << "\n[系统] 服务器已断开连接。\n";
            closesocket(clientSocket);
            WSACleanup();
            exit(0);
        }

        // 清理当前输入行再打印消息
        std::cout << "\r";  

        if (msg.targetID == -1) {
            std::cout << "【群聊】[" << msg.time << "] " << msg.name << ": " << msg.content << "\n";
        } else {
            std::cout << "【私聊】[" << msg.time << "] " << msg.name 
                      << " → 你" << ": " << msg.content << "\n";
        }

        std::cout << "> "; 
        std::cout.flush();
    }
}

int main() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);

    SOCKET clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = inet_addr(SERVER_IP);

    std::cout << "正在连接聊天室服务器...\n";
    if (connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cout << "连接失败！请确认服务器是否开启。\n";
        return -1;
    }
    std::cout << "连接成功！\n";

    char name[50];
    std::cout << "请输入你的昵称: ";
    std::cin.getline(name, sizeof(name));

    send(clientSocket, name, strlen(name) + 1, 0);

    int clientID;
    recv(clientSocket, (char*)&clientID, sizeof(clientID), 0);
    std::cout << "你的 ID: " << clientID << "\n\n";

    std::cout << "==================== 聊天室 ====================\n";
    std::cout << "输入 \"Private\" 进行私聊，输入 \"Quit\" 退出聊天\n";
    std::cout << "=================================================\n";

    std::thread(receiveMessages, clientSocket).detach();

    Message msg{};
    msg.senderID = clientID;
    strcpy(msg.name, name);

    char buffer[1024];

    while (true) {
        std::cout << "> ";
        std::cout.flush();

        if (!std::cin.getline(buffer, sizeof(buffer))) break;
        if (buffer[0] == '\0') continue;  // 空行跳过

        if (strcmp(buffer, "Quit") == 0) {
            std::cout << "正在退出聊天室...\n";
            break;
        }

        // 私聊
        if (strcmp(buffer, "Private") == 0) {
            std::cout << "请输入目标用户ID: ";
            std::cin >> msg.targetID;
            std::cin.ignore(10000, '\n');

            std::cout << "请输入私聊内容: ";
            std::cin.getline(msg.content, sizeof(msg.content));
        } else {
            msg.targetID = -1;
            strcpy(msg.content, buffer);
        }

        // 设置时间
        std::time_t now = std::time(nullptr);
        std::strftime(msg.time, sizeof(msg.time), "%Y-%m-%d %H:%M:%S", std::localtime(&now));

        send(clientSocket, (char*)&msg, sizeof(msg), 0);
    }

    closesocket(clientSocket);
    WSACleanup();
    return 0;
}
