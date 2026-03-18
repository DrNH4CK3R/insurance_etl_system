import mysql.connector
from mysql.connector import Error
import logging

# Import the credentials we set up in the config file
from config.db_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    A context manager for MySQL database connections.
    Ensures safe execution of queries and automatic connection closure.
    """
    def __init__(self):
        self.host = DB_HOST
        self.port = DB_PORT
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.database = DB_NAME
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """Establishes the connection when entering the 'with' block."""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.conn.is_connected():
                self.cursor = self.conn.cursor(dictionary=True)
                logger.info("Successfully connected to the MySQL database.")
                return self  # Returns the manager instance so you can use db.cursor
        except Error as e:
            logger.error(f"Failed to connect to MySQL database: {e}")
            raise e  # Stop execution if we can't connect to the database

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Executes when exiting the 'with' block. 
        Commits on success, rolls back on error, and always closes the connection.
        """
        # If an error occurred inside the 'with' block, exc_type will not be None
        if exc_type:
            logger.error(f"Database transaction aborted due to error: {exc_val}. Rolling back changes.")
            if self.conn and self.conn.is_connected():
                self.conn.rollback()
        else:
            # If everything went smoothly, commit the transaction
            if self.conn and self.conn.is_connected():
                self.conn.commit()
                logger.info("Database transaction committed successfully.")

        # The most crucial part: Always clean up resources
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("MySQL connection gracefully closed.")

    def execute_schema(self, script_path="report.sql"):
        """Utility method to run the initial SQL table creation script."""
        try:
            with open(script_path, 'r') as file:
                sql_script = file.read()
                
            sql_commands = sql_script.split(';')
            
            for command in sql_commands:
                cleaned_command = command.strip()
                if cleaned_command:
                    self.cursor.execute(cleaned_command)
                    
            logger.info(f"Successfully executed schema script: {script_path}")
        except Exception as e:
            logger.error(f"Failed to execute schema script {script_path}: {e}")
            raise e