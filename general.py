import pandas as pd


def q90(x):
    return "{:.0f}".format(x.quantile(0.90))


def q95(x):
    return "{:.0f}".format(x.quantile(0.95))


def q99(x):
    return "{:.0f}".format(x.quantile(0.99))


def throughput(x):
    throughput_result = 0
    total_time = x.min()
    total_rows = x.count()

    if not total_time == 0:
        throughput_result = total_rows / total_time

    return "{:.0f}".format(throughput_result)


def error_count(x):
    count = 0
    for status in x:
        if not status:
            count += 1
    return "{:.0f}".format(count)


def error_percentage(x):
    count = 0
    result = 0
    for status in x:
        if not status:
            count += 1

    if len(x) != 0:
        result = float(count / len(x)) * 100
    return "{:.0f}".format(result)


df = pd.read_csv('results.csv')
df['responseMessage'] = df['responseMessage'].fillna('NA')
df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='ms')
min_time = df['timeStamp'].min()
max_time = df['timeStamp'].max()

elapsed_time = pd.to_timedelta(max_time - min_time).seconds
df['elapsed_time'] = elapsed_time

aggregate_functions = {
    "elapsed": ["count", "min", "median", q90, q95, q99, "max"],
    "elapsed_time": throughput,
    "success": [error_count, error_percentage],
    "bytes": "median",
    "sentBytes": "median"
}

aggregate_result = df.groupby(['label', 'responseCode']).agg(aggregate_functions)
aggregate_result.columns = ['# Samples', 'min (ms)', 'median (ms)',
                            'q90 (ms)', 'q95 (ms)', 'q99 (ms)', 'max (ms)', 'Throughput/sec', 'Error Count',
                            'Error%', 'Avg Bytes received', 'Avg Bytes sent']

aggregate_result.to_csv("test.csv")
