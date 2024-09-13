import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from config import DB_PATH

sns.set(style="whitegrid")

def load_data():
    engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
    query = "SELECT transaction_date, SUM(total) as revenue FROM transactions GROUP BY transaction_date"
    df = pd.read_sql(query, con=engine)
    return df

def visualize_data(df):
    plt.figure(figsize=(14, 7))
    sns.lineplot(x='transaction_date', y='revenue', data=df, marker='o')
    plt.title('Доход по датам')
    plt.xlabel('Дата')
    plt.ylabel('Доход (руб.)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    df = load_data()
    visualize_data(df)
