# -*- coding: utf-8 -*-

import os
import shutil
import sys
from core.common import *
from core.datanode import DataNode
from core.namenode import NameNode
import tab

def process_cmd(cmd, gconf):
    """Parse and process user command."""

    cmds = cmd.split()
    flag = False
    if len(cmds) < 1 or cmds[0] not in operation_names:
        print('Usage: put|read|fetch|quit|ls|put2|read2|fetch2|ls2|mkdir|meta_chunks|meta_replicas|namenode_format|check|recover_chunks|recover_servers|rm')
        return False

    operation = cmds[0]
    try:
        if operation == 'put':
            if len(cmds) != 2:
                print('Usage: put source_file_path')
            elif not os.path.isfile(cmds[1]):
                print('Error: input file does not exist')
            else:
                gconf.file_path = cmds[1]
                gconf.cmd_type = COMMAND.put
                flag = True
        elif operation == 'read':
            if len(cmds) != 4:
                print('Usage: read file_id offset count')
            else:
                gconf.file_id, gconf.read_offset, gconf.read_count = map(int, cmds[1:])
                gconf.cmd_type = COMMAND.read
                flag = True
        elif operation == 'fetch':
            if len(cmds) != 3:
                print('Usage: fetch file_id save_path')
            else:
                gconf.fetch_savepath = cmds[2]
                base = os.path.split(gconf.fetch_savepath)[0]
                if base and not os.path.exists(base):
                    print('Error: input save_path does not exist')
                else:
                    gconf.file_id = int(cmds[1])
                    gconf.cmd_type = COMMAND.fetch
                    flag = True
        elif operation == 'quit':
            print("Bye: Exiting miniDFS...")
            sys.exit(0)
        elif operation == 'ls':
            gconf.cmd_type = COMMAND.ls
            flag = True
        elif operation == 'mkdir':
            if len(cmds) != 2:
                print('Usage: mkdir file_dir')
            else:
                gconf.file_dir = cmds[1]
                gconf.cmd_type = COMMAND.mkdir
                flag = True
        elif operation == 'meta_chunks':
            gconf.cmd_type = COMMAND.meta_chunks
            flag = True
        elif operation == 'meta_replicas':
            gconf.cmd_type = COMMAND.meta_replicas
            flag = True
        elif operation == 'namenode_format':
            gconf.cmd_type = COMMAND.namenode_format
            flag = True
        elif operation == 'check':
            gconf.cmd_type = COMMAND.check
            flag = True
        elif operation == 'recover_chunks':
            gconf.cmd_type = COMMAND.recover_chunks
            flag = True
        elif operation == 'recover_servers':
            gconf.cmd_type = COMMAND.recover_servers
            flag = True
        elif operation == 'rm':
            if len(cmds) != 2:
                print('Usage: rm file_id')
            else:
                gconf.file_id = int(cmds[1])
                gconf.cmd_type = COMMAND.rm
                flag = True
        else:
            print('Unknown command')
    except ValueError:
        print('Error: One or more arguments have the wrong format. Please check again.')

    return flag

def setup_directories():
    """Ensure necessary directories are set up."""
    if not os.path.isdir("dfs"):
        os.makedirs("dfs")
        for i in range(NUM_DATA_SERVER):
            os.makedirs(f"dfs/datanode{i}")
        os.makedirs("dfs/namenode")
    else:
        for i in range(NUM_DATA_SERVER):
            if not os.path.isdir(f"dfs/datanode{i}"):
                os.makedirs(f"dfs/datanode{i}")
        if not os.path.isdir("dfs/namenode"):
            os.makedirs("dfs/namenode")

def start_nodes(gconf):
    """Start the NameNode and DataNodes."""
    name_server = NameNode('NameServer', gconf)
    name_server.start()

    data_servers = [DataNode(s_id, gconf) for s_id in range(NUM_DATA_SERVER)]
    for server in data_servers:
        server.start()

def run():
    """Main loop to process user commands."""
    gconf = GlobalConfig()

    # Ensure directories are properly set up
    setup_directories()

    # Start NameNode and DataNodes
    start_nodes(gconf)

    cmd_prompt = 'MiniDFS > '
    print(cmd_prompt, end='')

    while True:
        cmd_str = input()
        gconf.cmd_flag = process_cmd(cmd_str, gconf)
        
        if gconf.cmd_flag:
            # Trigger the command execution
            gconf.name_event.set()
            if gconf.cmd_type in [COMMAND.put, COMMAND.put2]:
                for i in range(NUM_DATA_SERVER):
                    gconf.main_events[i].wait()
                if gconf.file_id is None:
                    print('Put failed! Please check your file path.')
                else:
                    print(f'Put succeed! File ID is {gconf.file_id}')
                gconf.server_chunk_map.clear()
                for i in range(NUM_DATA_SERVER):
                    gconf.main_events[i].clear()

            # Handle other command types as necessary
            if gconf.cmd_type == COMMAND.mkdir:
                gconf.mkdir_event.wait()
                gconf.mkdir_event.clear()

        print(cmd_prompt, end='')


def start_stop_info(operation):
    """Display start or stop information."""
    print(f'{operation} NameNode')
    for i in range(NUM_DATA_SERVER):
        print(f'{operation} DataNode{i}')


if __name__ == '__main__':
    start_stop_info('Start')
    run()
