# scripts/analysis/visualization.py

import matplotlib.pyplot as plt
from analytics import monthly_revenue, top_products, get_engine

def plot_monthly_revenue(df):
    """
    Bar chart of monthly revenue.
    """
    df["period"] = df.apply(lambda r: f"{r.year}-{r.month:02}", axis=1)
    plt.figure()
    plt.bar(df["period"], df["revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Monthly Revenue")
    plt.xlabel("Year-Month")
    plt.ylabel("Revenue (GBP)")
    plt.tight_layout()
    out = "output/monthly_revenue.png"
    plt.savefig(out)
    plt.close()
    print(f"✅ Saved monthly revenue chart to {out}")

def plot_top_products(df):
    """
    Horizontal bar chart of top products by revenue.
    """
    plt.figure()
    plt.barh(df["description"], df["revenue"])
    plt.title("Top Products by Revenue")
    plt.xlabel("Revenue (GBP)")
    plt.tight_layout()
    out = "output/top_products.png"
    plt.savefig(out)
    plt.close()
    print(f"✅ Saved top products chart to {out}")

if __name__ == "__main__":
    eng = get_engine()
    rev = monthly_revenue(eng)
    top = top_products(eng)
    plot_monthly_revenue(rev)
    plot_top_products(top)
