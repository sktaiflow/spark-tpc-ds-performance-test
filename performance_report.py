import matplotlib.pyplot as plt
import pandas as pd

scale = "100g"

k8_dir = f"report/{scale}-kubernetes"
yarn_dir = f"report/{scale}-yarn"

res_files = [f"query{str(i).zfill(2)}" for i in range(1, 99)]

summary_file = "run_summary.txt"


def performance():

    k8s_summary = f"{k8_dir}/{summary_file}"
    yarn_summary = f"{yarn_dir}/{summary_file}"

    k8s_df = pd.read_csv(
        k8s_summary,
        skiprows=3,
        delim_whitespace=True,
        header=None,
        names=["Query", "Time", "Rows"],
    )
    yarn_df = pd.read_csv(
        yarn_summary,
        skiprows=3,
        delim_whitespace=True,
        header=None,
        names=["Query", "Time", "Rows"],
    )

    # 데이터 병합
    merged_df = pd.merge(
        k8s_df, yarn_df, on=["Query", "Rows"], suffixes=("_k8s", "_yarn")
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
    plt.title("Kubernetes vs Yarn Query Performance")
    plt.legend()
    plt.tight_layout()
    plt.savefig("performance_comparison.png")
    plt.show()

    # 배율 계산
    merged_df["Speedup_Yarn"] = merged_df["Time_k8s"] / merged_df["Time_yarn"]
    merged_df["Speedup_K8s"] = merged_df["Time_yarn"] / merged_df["Time_k8s"]

    # yarn이 더 빠른 배율이 큰 쿼리 5개
    top_5_yarn_faster = merged_df.nlargest(5, "Speedup_Yarn")

    # kubernetes가 더 빠른 배율이 큰 쿼리 5개
    top_5_k8s_faster = merged_df.nlargest(5, "Speedup_K8s")

    print("Top 5 queries where Yarn is faster:")
    for _, row in top_5_yarn_faster.iterrows():
        print(f"{row['Query']}: {row['Speedup_Yarn']:.2f}x faster")

    print("\nTop 5 queries where Kubernetes is faster:")
    for _, row in top_5_k8s_faster.iterrows():
        print(f"{row['Query']}: {row['Speedup_K8s']:.2f}x faster")


performance()

