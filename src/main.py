import json
import urllib.parse
import util
import getpass

csrf = 'DfDAGsq68lj08TuVEjlkgPNQsESYXwCTmOnrOjoNd6NQxgEfvfCbYCYzOMwq3dus'

base = 'http://localhost:8081/omnidb'


if __name__ == "__main__":
    uname = input("Enter Username: ")
    pw = getpass.getpass(f"Enter Password for '{uname}':")

    # login
    sessionid = util.login(csrf, base, uname, pw)
