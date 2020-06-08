import sys

from raftsies.server import RaftNode


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('./run.py node_num')
        sys.exit(1)

    node_num = int(sys.argv[1])
    raft_node = RaftNode(node_num)
    raft_node.start()
