# cs568-reliable-udp


Reliable Data Transfer Protocol (RDTP) Client
================================================================

This project is a client-side implementation of a Reliable Data Transfer Protocol built on top of UDP. Developed for the CS568 - Contemporary Topics in Networking course, the client is designed to download files reliably and efficiently from a server using multi-path transmission (simultaneous connections over two different ports).

----------------------------------------------------------------
FEATURES
----------------------------------------------------------------
* Multi-Path Downloading: Utilizes two network paths (or ports) simultaneously to maximize throughput (port 5000 and 5001 over Wi-Fi).
* Reliability over UDP: Implements custom mechanisms for packet loss recovery, retransmission, and flow control (Sliding Window).
* Congestion Control: Features an AIMD-style (Additive Increase, Multiplicative Decrease) algorithm to adapt to network conditions. When the network is too busy, the data transfer speed is halved. Otherwise it increases slowly (similar logic with TCP)
* Data Integrity: Verifies downloaded files using MD5 checksums to ensure zero corruption.
* Handshake Protocol: Implements a strict request/response handshake (File List -> File Size -> File Data).

----------------------------------------------------------------
PREREQUISITES
----------------------------------------------------------------
To run this project, you need a Linux-based environment.

* OS: Ubuntu 20.04+ (or WSL2 on Windows 10/11).
* Java: JDK 17 or higher.
* Build Tools: Maven (for server), standard Java compiler (for client).
* IDE: Any IDE is suitable for this project. We preferred to use VS code

[Installing Dependencies on Ubuntu/WSL]
Run the following commands:
   sudo apt update
   sudo apt install openjdk-17-jdk git maven -y

----------------------------------------------------------------
INSTALLATION & COMPILATION
----------------------------------------------------------------
1. Clone the Repository:
   git clone https://github.com/streaming-university/public-teaching-rdtp.git
   cd public-teaching-rdtp/FileListClient

2. Place the Client Code:
   Ensure your 'rudp.java' file is located in the 'src/client/' directory.
   Check your 'rudp.java' file from your IDE. If you use VS code, you can simply type "code ." into the terminal (from the FileListClient directory). Don't forget to press Ctrl + S 

3. Compile the Client:
   Run the following command from the FileListClient directory:
   javac -cp "src:lib/*" src/client/rudp.java

----------------------------------------------------------------
USAGE
----------------------------------------------------------------

[1. Setting Up the Server (For Testing)]
To test the client, you must have the FileListServer running on two separate ports (e.g., 5000 and 5001).

Option A: Remote Server (AWS/VM) - Recommended
Before connecting via SSH, you must secure your private key (.pem file) to avoid permission errors. 

If you want to implement this project using WSL/WSL2, follow these steps:

1.1- Download the rudp.pem file and move it to the desktop. Then fetch it from the desktop. Run this command to do it: 
   cp /mnt/c/Users/<YOUR_NAME>/Desktop/rudp.pem .

1.2- Run this command:
   chmod 400 rudp.pem

1.3- SSH into your remote server and start two instances in separate terminals:
   # Terminal 1 (Port 5000)
   java -cp "target/classes:target/lib/*:conf" server.FileListServer 5000

   # Terminal 2 (Port 5001)
   java -cp "target/classes:target/lib/*:conf" server.FileListServer 5001

[2. Running the Client]
Open your terminal in the 'FileListClient' directory and run the client by providing the server's IP address and both ports.

Command Format:
   java -cp "src:lib/*" client.rudp <SERVER_IP>:5000 <SERVER_IP>:5001

Example (AWS):
   java -cp "src:lib/*" client.rudp 13.60.7.85:5000 13.60.7.85:5001

Example (Localhost):
   java -cp "src:lib/*" client.rudp 127.0.0.1:5000 127.0.0.1:5001

----------------------------------------------------------------
HOW IT WORKS
----------------------------------------------------------------
1. Initialization: The client starts and asks for the Server IP and Ports.
2. File List: It sends a Type 1 request to fetch the list of available files.
3. File Size: Upon user selection, it sends a Type 2 request to get the total size of the file.
4. Download: It splits the file into chunks and requests them via Type 3 requests using both paths in parallel.
5. Verification: Once the download is complete, it calculates the MD5 hash of the local file and compares it with the server's hash.

----------------------------------------------------------------
IMPORTANT NOTES
----------------------------------------------------------------
* Windows Users: Do not run this directly in Command Prompt or PowerShell. The traffic control (TC) mechanisms and network socket behaviors require a Linux kernel. Please use WSL2.
* Firewall: If using a remote server (AWS), ensure that UDP ports 5000-5001 and TCP port 22 (SSH) are open in the Security Group settings.
