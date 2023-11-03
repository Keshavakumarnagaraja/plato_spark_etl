import unittest
from unittest.mock import patch
from etl_manager import EtlManager

class TestEtlManager(unittest.TestCase):
    @patch('etl_manager.SetupDb')
    @patch('etl_manager.FileHandler')
    def test_check_create_database(self, mock_file_handler, mock_database_setup):
        configs = {'key': 'value', 'pg_db': {'host': 'localhost', 'port': '5432', 'database': 'mydatabase', 'user': 'myuser', 'password': 'mypassword'}}
        etl_manager_instance = EtlManager(configs)
        etl_manager_instance.check_create_database()
        mock_database_setup.assert_called_once_with(configs)
        mock_database_setup.return_value.create_db_tables.assert_called_once()

if __name__ == '__main__':
    unittest.main()
