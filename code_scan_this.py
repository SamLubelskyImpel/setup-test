import os
import hashlib
import sqlite3

# Hardcoded secret
API_KEY = "1234567890abcdef"  # Sensitive information hardcoded

def insecure_hash(password):
    """
    Uses the insecure MD5 hashing algorithm to hash passwords.
    """
    return hashlib.md5(password.encode()).hexdigest()  # MD5 is insecure and should not be used

def sql_injection_example(user_input):
    """
    Demonstrates a SQL injection vulnerability.
    """
    connection = sqlite3.connect("example.db")
    cursor = connection.cursor()
    
    # Vulnerable query with string concatenation
    query = f"SELECT * FROM users WHERE username = '{user_input}'"
    cursor.execute(query)  # SQL Injection possible here!
    result = cursor.fetchall()
    print(result)
    connection.close()

def dangerous_os_command(user_input):
    """
    Demonstrates a vulnerability by using unsanitized input in an OS command.
    """
    os.system(f"echo User input: {user_input}")  # Command injection risk

def main():
    print("Starting the vulnerable application...")

    # Example 1: Hardcoded API key
    print(f"Using API Key: {API_KEY}")

    # Example 2: Insecure hashing
    password = "user_password"
    hashed_password = insecure_hash(password)
    print(f"Insecurely hashed password: {hashed_password}")

    # Example 3: SQL Injection
    user_input = input("Enter your username: ")
    sql_injection_example(user_input)

    # Example 4: Command injection
    command_input = input("Enter a command: ")
    dangerous_os_command(command_input)

if __name__ == "__main__":
    main()
