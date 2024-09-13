from sqlalchemy import create_engine
import pandas as pd
from config import DB_PATH


def analyze_data():
    engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
    query = """
    SELECT 
        product_name, 
        SUM(quantity) AS total_sold, 
        SUM(total) AS revenue
    FROM transactions
    GROUP BY product_name
    ORDER BY revenue DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, con=engine)
    print("Топ 10 товаров по доходу:")
    print(df)


if __name__ == '__main__':
    analyze_data()
