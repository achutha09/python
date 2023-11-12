# This class is an example of using PyTest. Creating unit tests for the assignment is not required.  This is for reference.
import unittest
import pandas as pd

class UnitTestSample(unittest.TestCase):

# One useful pattern when learning Python is to create simple unit tests to make sure you understand the concept.
# That's not the typical unit test application, but it can be very helpful to isolate the concept from the application.
# This test simply demonstrates how to subset a dataframe columns.

# The AAA pattern (Arrange-Act-Assert) is a common approach to unit testing.  It is a helpful reminder for keeping the test
# functions granular and simple.

    def test_select_columns(self):
        # Arrange
        df = pd.DataFrame()
        df['Customer'] = ['Acme', 'Boo', 'Axe', 'Boo', 'Cat']
        df['Group'] = ['A', 'B', 'A', 'B', 'C']
        df['Price'] = [10, 10, 20, 30, 40]

        # Act - Subset using column names.
        df2 = df[['Customer', 'Group']]

        # Assert - Get column count
        self.assertEqual(df2.shape[1], 2)

if __name__ == '__main__':
    unittest.main()