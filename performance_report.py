import matplotlib.pyplot as plt
import pandas as pd


def performance(scale: str):
    k8_dir = f"report/{scale}-kubernetes"
    yarn_dir = f"report/{scale}-yarn"

    res_files = [f"query{str(i).zfill(2)}" for i in range(1, 99)]

    summary_file = "run_summary.txt"
    k8s_summary = f"{k8_dir}/{summary_file}"
    yarn_summary = f"{yarn_dir}/{summary_file}"

    k8s_df = (
        pd.read_csv(
            k8s_summary,
            skiprows=3,
            delim_whitespace=True,
            header=None,
            names=["Query", "Time", "Rows"],
        )
        .groupby("Query", as_index=False)
        .sum()
    )

    yarn_df = (
        pd.read_csv(
            yarn_summary,
            skiprows=3,
            delim_whitespace=True,
            header=None,
            names=["Query", "Time", "Rows"],
        )
        .groupby("Query", as_index=False)
        .sum()
    )

    # 데이터 병합
    merged_df = pd.merge(
        yarn_df,
        k8s_df,
        how="left",
        on=["Query", "Rows"],
        suffixes=(
            "_yarn",
            "_k8s",
        ),
    )
    assert not merged_df.empty, "Data merge resulted in an empty DataFrame"

    # 성능 비교 차트 생성
    queries = merged_df["Query"]
    k8s_times = merged_df["Time_k8s"]
    yarn_times = merged_df["Time_yarn"]

    x = range(len(queries))
    plt.figure(figsize=(10, 5))
    plt.plot(x, k8s_times, label="Kubernetes", marker="o")
    plt.plot(x, yarn_times, label="Yarn", marker="o")
    plt.xticks(x, [q.replace("query", "") for q in queries], rotation="vertical")
    plt.xlabel("Query")
    plt.ylabel("Time (s)")
    plt.title(f"Kubernetes vs Yarn Query Performance ({scale})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"performance_report_{scale}.png")
    plt.show()

    # Row 컬럼 제거
    merged_df = merged_df.drop(columns=["Rows"])

    # 증감률 계산
    merged_df["Change_Rate"] = round(
        (merged_df["Time_k8s"] - merged_df["Time_yarn"]) / merged_df["Time_yarn"] * 100,
        2,
    )

    # 평균 증감률
    avg_change_rate = merged_df["Change_Rate"].mean()
    print(f"Average change rate: {avg_change_rate:.2f}%")

    avg_row = pd.DataFrame([{"Query": "Average", "Change_Rate": avg_change_rate}])
    merged_df = pd.concat([merged_df, avg_row], ignore_index=True)
    merged_df.to_csv(f"performance_report_{scale}.csv", index=False)


performance("1g")
performance("100g")
