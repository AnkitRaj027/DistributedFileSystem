# Distributed File System (DFS)

A robust, fault-tolerant distributed file system implemented in Python. This system ensures data availability and integrity across multiple nodes, handling file chunking, replication, and automatic recovery. 

## Features
- **File Chunking & Replication**: Splits files into smaller chunks and replicates them across multiple storage nodes to ensure redundancy.
- **Heartbeat Monitoring**: Constantly monitors the health of storage nodes.
- **Automatic Recovery**: Automatically re-replicates data if a node fails.
- **Premium Web Dashboard**: A modern, glassmorphism-inspired UI for real-time monitoring and system control.
- **CLI Client**: A command-line interface for direct interaction with the system.

## Setup and Running

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Application**:
   ```bash
   python app.py
   ```

3. **Access the Dashboard**:
   Open your browser and navigate to `http://localhost:5000` (or the port specified in your console).

## Collaboration Workflow

1. `git pull origin main` to get the latest code.
2. Make your changes locally.
3. `git add .` to stage your changes.
4. `git commit -m "Your descriptive message here"` to commit.
5. `git push origin main` to push changes to GitHub.
