import plotly.express as px

def plot_stock_price(df, symbol, start_date, end_date):
    fig = px.line(df, x='date', y='close',
                    title=f'{symbol} Closing Prices ({start_date} to {end_date})')
    fig.update_layout(xaxis_title='Date', yaxis_title='Closing Price')
    fig.show()
