import sys
from pysyncobj import SyncObj, replicated

class ATMNetwork(SyncObj):
    def __init__(self, selfNode, partnerNodes):
        super(ATMNetwork, self).__init__(selfNode, partnerNodes)
        # This dictionary will be used to store account balances
        self.account_balances = {'Ishan': 1000, 
                                 'Shrinivas': 1000,
                                 'Mayank': 1000,
                                 'Shashwat': 1000,
                                 'Yashica': 1000
                            }

    # Withdrawal endpoint
    @replicated
    def withdrawal(self, account, amount):
        if account in self.account_balances and amount <= self.account_balances[account]:
            self.account_balances[account] -= amount
            return 'Withdrawal successful'
        else:
            return 'Withdrawal failed'

    # Deposit endpoint
    @replicated
    def deposit(self, account, amount):
        if account in self.account_balances:
            self.account_balances[account] += amount
            return 'Deposit successful'
        else:
            return 'Deposit failed'

    # Balance enquiry endpoint
    def balance(self, account):
        if account in self.account_balances:
            return self.account_balances[account]
        else:
            return 'Account not found'

    # Transfer endpoint
    @replicated
    def transfer(self, account1, account2, amount):
        if account1 in self.account_balances and account2 in self.account_balances and amount <= self.account_balances[account1]:
            self.account_balances[account1] -= amount
            self.account_balances[account2] += amount
            return 'Transfer successful'
        else:
            return 'Transfer failed'

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    port = int(sys.argv[1]) # The address of the current node
    partners = ['127.0.0.1:%d' % int(p) for p in sys.argv[2:]] # The addresses of the partner nodes

    network = ATMNetwork('127.0.0.1:%d' % port, partners)
    # print(network._getLeader())

    # Interactive mode for user input
    import time
    while True:
        time.sleep(1)
        print(network._isReady())
        # print('ATM network commands:')
        # print('1. Withdrawal')
        # print('2. Deposit')
        # print('3. Balance enquiry')
        # print('4. Transfer')
        # print('5. Exit')
        # command = input('Enter command number: ')
        # if network._getLeader() is None:
        #     print("Run other atms first")
        #     continue
        # print(network._isLeader())
        # if command == '1':
        #     account = input('Enter account name: ')
        #     amount = int(input('Enter withdrawal amount: '))
        #     result = network.withdrawal(account, amount)
        #     print(result)
        # elif command == '2':
        #     account = input('Enter account name: ')
        #     amount = int(input('Enter deposit amount: '))
        #     result = network.deposit(account, amount)
        #     print(result)
        # elif command == '3':
        #     account = input('Enter account name: ')
        #     result = network.balance(account)
        #     print(result)
        # elif command == '4':
        #     account1 = input('Enter account name to transfer from: ')
        #     account2 = input('Enter account name to transfer to: ')
        #     amount = int(input('Enter transfer amount: '))
        #     result = network.transfer(account1, account2, amount)
        #     print(result)
        # elif command == '5':
        #     break
        # else:
        #     print('Invalid command number')
