class Bankaccount():
    def __init__(self,balance,credit):
        self.balance = balance 
        self.credit = credit 
    def deposite(self,amount):
        self.balance = self.balance + amount 
    def loan_amount(self):
        return 2000
    def get_add_amount(self):
        return self.credit 
    def get_total_amount(self):
        return self.get_add_amount() + self.balance 
  
get_amount =Bankaccount(1000,3000) 
print(get_amount.balance)
print(get_amount.credit)
get_amount.deposite(5000) 
print(get_amount.balance)
print(get_amount.get_add_amount())
print(get_amount.get_total_amount()) 
print(get_amount.loan_amount())