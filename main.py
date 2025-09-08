#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def main():
    """主程序入口，处理SQL输入并执行"""
    while True:
        try:
            # 读取SQL输入
            sql = input('miniDB> ')
            if sql.lower() in ('exit', 'quit'):
                break
                
            # TODO: 实现SQL解析和执行
            print(f'执行SQL: {sql}')
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    print('Welcome to MiniDB!')
    main()