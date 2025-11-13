import os
import argparse
import pandas as pd
from pandas import DataFrame as df, read_pickle
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
import matplotlib
from matplotlib import rcParams

# Increase all font sizes by 8 points from their defaults
rcParams.update({key: rcParams[key] + 8 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

def make_dict(names, args, operate):
    dict = {}
    for i, j, k in zip(names, args, operate):
        dict[i] = k(j)
    return dict

def strip_eq(x):
    return int(x[x.find("=") + 1:])

def get_type(x):
    if x.startswith("nvme0n2"):
        return "ZNS"
    if x.startswith("nvme1n1p1"):
        return "SSD"

titles = [
    "chunk_size",
    "latency",
    "distribution",
    "ratio",
    "iterations",
    "zones",
    "type"
]

operate = [
    int,
    strip_eq,
    lambda i : i,
    strip_eq,
    strip_eq,
    strip_eq,
    get_type
]

def ParseData(directory_path):
    runs = []
    data = {}
    key = 0
    for filename in os.listdir(directory_path):
        full_path = os.path.join(directory_path, filename)
        run = pd.DataFrame(data = [make_dict(titles, filename.split(","), operate)])
        run["id"] = key
        run = run.set_index('id')

        for data_file in os.listdir(full_path):
            type = os.path.splitext(data_file)[0]
            if type not in data:
                data[type] = pd.DataFrame()

            f = open(os.path.join(full_path, data_file))
            value = (pd.read_csv(f, names=["time", "name", "value"])
                     .drop("name", axis=1))
            value["id"] = key
            value.set_index('id')
            data[type] = pd.concat([data[type], value])
            print("data", type, "parsed")

        key += 1
        runs.append(run)
        print("run", key, "done:", full_path)

    run_file = pd.concat(runs)
    run_file.to_pickle("run_file")
    for type, data_file in data.items():
        data_file.to_pickle(f"{type}.data")
    return (run_file, data)

def ReadData(dir_path):
    run_file = read_pickle(os.path.join(dir_path, "run_file"))
    data = {}
    for filename in os.listdir(dir_path):
        full_path = os.path.join(dir_path, filename)
        root, ext = os.path.splitext(os.path.basename(full_path))
        if ext == ".data":
            print(f"reading {filename}")
            data[root] = read_pickle(full_path)
    return (run_file, data)

def GenerateGraph(runfile, data, analysis, title, scale, genpdf_name):
    print(f"Generating {title} from {analysis}, output is {genpdf_name}")
    # font = {'size'   : 12}

    # matplotlib.rc('font', **font)
    distribution = ["ZIPFIAN", "UNIFORM"]
    distrib_hatch = ['O', '\\']
    chunk_size = [536870912,65536]
    chunk_color = ["lightgreen", "lightblue"]
    ratio = [2, 10]
    type = ["ZNS", "SSD"]

    idx = 0
    fig, axes = plt.subplots(1, 8, figsize=(20, 6))
    for c, cc in zip(chunk_size, chunk_color):
        for d, dh in zip(distribution, distrib_hatch):
            for r in ratio:
                current_data = []
                ids = []
                for t in type:
                    ids.append(runfile[(runfile["type"] == t) &
                                       (runfile["ratio"] == r) &
                                       (runfile["chunk_size"] == c) &
                                       (runfile["distribution"] == d)].index[0])
                cur = data[analysis]
                zns = cur[cur["id"] == ids[0]]
                ssd = cur[cur["id"] == ids[1]]
                current_data.append(zns["value"].to_numpy()*scale)
                current_data.append(ssd["value"].to_numpy()*scale)

                bp = axes[idx].boxplot(current_data,
                                  showfliers=False,
                                  widths=1.0,
                                  medianprops=dict(linewidth=2, color='red'),
                                      patch_artist=True)
                # axes[idx].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                axes[idx].set_xticks([1, 2])
                axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=20)

                # axes[idx].set_xlabel(f"1:{r}")
                ylim = axes[idx].get_ylim()
                axes[idx].text(1.5, ylim[1],  # Centered above both boxes
                               f"Ratio: 1:{r}", ha='center', va='bottom', fontsize=18, weight='bold')

                # axes[idx].set_ylabel("GB/s", rotation=90)
                for label in axes[idx].get_yticklabels():
                    label.set_rotation(45)
                # plt.suptitle(title)
                bp['boxes'][0].set_hatch(dh)
                bp['boxes'][1].set_hatch(dh)
                bp['boxes'][0].set_facecolor(cc)
                bp['boxes'][1].set_facecolor(cc)
                idx += 1

    plt.subplots_adjust(wspace=0.0, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.94)
    # Alpha=.99 is due to a bug with pdf export
    legends = [mpatches.Patch(facecolor='lightgreen', hatch='O', label='Zipfian 512MiB', alpha=.99),
               mpatches.Patch(facecolor='lightgreen', hatch='\\', label='Uniform 512MiB', alpha=.99),
               mpatches.Patch(facecolor='lightblue', hatch='O', label='Zipfian 64KiB', alpha=.99),
               mpatches.Patch(facecolor='lightblue', hatch='\\', label='Uniform 64KiB', alpha=.99)]

    fig.legend(
        ncols=4,
        handles=legends,
        bbox_to_anchor=(0.5, 0.02),  # Center horizontally at bottom of figure
        loc='center',
        fontsize="x-large",
        columnspacing=2.0,  # More space between columns
        frameon=False  # Remove legend frame
    )
    plt.savefig(genpdf_name, bbox_inches='tight')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate graphs. The data should be split already and each run should be in a separate folder.")
    parser.add_argument('-f', '--filename', required=True, help='The directory to either read the data from, or the location where the pickled data is')
    parser.add_argument('-r', action='store_true', required=False, help='Turn this on if you want to read the data from scratch')
    args = parser.parse_args()

    # Stores data about the run (e.g. type of ssd, chunk size)
    runfile = None
    # Stores the actual runtime data
    data = None

    if args.r:
        runfile, data = ParseData(args.filename)
    else:
        runfile, data = ReadData(".")

    GenerateGraph(runfile, data, "CACHETHROUGHPUT", "Throughput (GiB/s)", 1/2**30, "cache_throughput.png")
    GenerateGraph(runfile, data, "GETLATENCY_EVERY", "Latency (ms)", 1/10**6, "get_latency.png")
