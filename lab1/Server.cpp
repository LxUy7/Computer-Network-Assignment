#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <winsock2.h>
#include <ws2tcpip.h>
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

using namespace std;

const int PORT = 8080;
vector<pair<SOCKET, int>> clients;
mutex clients_mutex;
int nextID = 1;

void Broadcast(const Message& msg, SOCKET exclude = INVALID_SOCKET) {
    lock_guard<mutex> lock(clients_mutex);
    for (const auto& c : clients) {
        if (c.first != exclude) {
            send(c.first, (char*)&msg, sizeof(msg), 0);
        }
    }
}

void SendTo(int targetID, const Message& msg) {
    lock_guard<mutex> lock(clients_mutex);
    for (const auto& c : clients) {
        if (c.second == targetID) {
            send(c.first, (char*)&msg, sizeof(msg), 0);
            break;
        }
    }
}

void HandleClient(SOCKET client) {
    int clientID = nextID++;
    send(client, (char*)&clientID, sizeof(clientID), 0);

    char name[50] = "匿名";
    recv(client, name, sizeof(name), 0);
    name[49] = '\0';

    {
        lock_guard<mutex> lock(clients_mutex);
        clients.emplace_back(client, clientID);
    }

    cout << "【" << name << "】(ID: " << clientID << ") 加入了聊天室\n";

    Message msg;
    while (recv(client, (char*)&msg, sizeof(msg), 0) > 0) {
        msg.name[49] = '\0';
        msg.content[511] = '\0';

        if (msg.targetID == -1) {
            Broadcast(msg, client);
            cout << "[" << msg.time << "] " << msg.name << ": " << msg.content << endl;
        } else {
            SendTo(msg.targetID, msg);
            cout << "[" << msg.time << "] " << msg.name << " -> " << msg.targetID << ": " << msg.content << endl;
        }
    }

    {
        lock_guard<mutex> lock(clients_mutex);
        clients.erase(remove_if(clients.begin(), clients.end(),
            [client](auto& c) { return c.first == client; }), clients.end());
    }
    cout << "【" << name << "】已退出\n";
    closesocket(client);
}

int main() {
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);

    SOCKET server = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server, (sockaddr*)&addr, sizeof(addr));
    listen(server, SOMAXCONN);

    cout << "聊天室服务器已启动！监听端口: " << PORT << endl;
    cout << "等待用户加入...\n";

    while (true) {
        SOCKET client = accept(server, nullptr, nullptr);
        if (client != INVALID_SOCKET) {
            thread(HandleClient, client).detach();
        }
    }
}