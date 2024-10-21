import trak

trak.pd.set_option("display.max_rows", 1000)
df = trak.log_parser("/var/log/nginx/access.log")
print(
    df[df["status_code"] == "200"].pivot_table(
        "ip", "url", "status_code", aggfunc="nunique"
    )
)
