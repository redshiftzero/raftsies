import sys

from raftsies.server import RaftNode


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('./run.py node_num initial_state')
        sys.exit(1)

    node_num = int(sys.argv[1])
    state = sys.argv[2]
    raft_node = RaftNode(node_num, state)
    raft_node.start()
