import sqlite3
import os.path
from os.path import isdir
from os import mkdir
import numpy as np
import matplotlib as mpl
mpl.use('Agg')

import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import sys
import math
import random

from matplotlib import colors as mcolors
colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
                for name, color in colors.items())
sorted_names = [name for hsv, name in by_hsv]
random.shuffle(sorted_names)
sorted_names.clear()
sorted_names.append("magenta")
sorted_names.append("midnightblue")
sorted_names.append("deepskyblue")
sorted_names.append("cadetblue")
sorted_names.append("navy")
sorted_names.append("mediumblue")
sorted_names.append("slateblue")
#sorted_names.append("mediumslateblue")
#sorted_names.append("blueviolet")
#sorted_names.append("navy")
#sorted_names.append("peru")
#sorted_names.append("aquamarine")
#sorted_names.append("turquoise")
#sorted_names.append("lightseagreen")
#sorted_names.append("mediumturquoise")
#sorted_names.append("darkcyan")
#sorted_names.append("c")
#sorted_names.append("cyan")
#sorted_names.append("darkturquoise")
sorted_names.append("rebeccapurple")
sorted_names.append("blueviolet")
sorted_names.append("indigo")
sorted_names.append("darkviolet")
sorted_names.append("mediumorchid")
sorted_names.append("plum")
sorted_names.append("violet")
sorted_names.append("purple")
#sorted_names.append("blue")
#sorted_names.append("cadetblue")
#sorted_names.append("mediumspringgreen")
#sorted_names.append("blanchedalmond")
#sorted_names.append("magenta")
#sorted_names.append("azure")
#sorted_names.append("darkgrey")
#sorted_names.append("cornflowerblue")
#sorted_names.append("plum")
#sorted_names.append("royalblue")
#sorted_names.append("magenta")
#sorted_names.append("mediumslateblue")
#sorted_names.append("blueviolet")
#sorted_names.append("navy")
#sorted_names.append("peru")
sorted_names.append("deepskyblue")
sorted_names.reverse()





font = {'weight' : 'normal',
            'size'   : 12}
mpl.rc('font', **font)

def to_string(algorithm, label):
    return algorithm + "::" + label

def to_string2(dataset, algorithm):
    return  str(dataset) + "::" + algorithm

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def RepresentsFloat(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False

def avg(l):
    return sum(l)/len(l)

def autolabel(rects, ax):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

def read_from_file(filename, dataraw, data, datastd, labels):
    with open(filename) as f:
        algorithm = ''
        backoff = ''
        ts_group_length = ''
        read_block_length = ''
        for line in f:
            if 'version' in line:
                algorithm = line.split()[2] + '-' + read_block_length
            elif 'Backoff' in line:
                if ts_group_length != '':
                    algorithm = algorithm.split('-')[0] + '-' + read_block_length + '-' + ts_group_length  + '-' + line.split()[3]
            elif 'ts-group-length' in line:
                ts_group_length = line.split()[1]
            elif 'Read Block Length' in line:
                read_block_length = line.split()[3]
            elif(RepresentsFloat(line.split()[0])):
                for i, label in labels:
                    key = to_string(algorithm, label)
                    if key not in dataraw:
                        dataraw[key] = []
                    dataraw[key].append(float(line.split()[i]))
                    i+=1
    
    for key in dataraw:
        
        #dataraw[key].remove(max(dataraw[key]))
       # dataraw[key].remove(max(dataraw[key]))
       # dataraw[key].remove(max(dataraw[key]))
       # dataraw[key].remove(max(dataraw[key]))

        #dataraw[key].remove(min(dataraw[key]))
       # dataraw[key].remove(min(dataraw[key]))
        # dataraw[key].remove(min(dataraw[key]))
        # dataraw[key].remove(min(dataraw[key]))        

        if "nodes" in key or "help-avoided" in key:
            data[key] = avg(dataraw[key])
            datastd[key] = np.std(dataraw[key])
        else:
            data[key] = avg(dataraw[key]) / 1000000
            datastd[key] = np.std(dataraw[key]) / 1000000
        # print ("adding data[" + key + "] = " + str(avg(dataraw[key])))
    return algorithm
    
    



def plot_line_x_thread(title, outdir, name, ylabel, algorithms, labels, data, datastd):

    # data = totaltime

    series = {}
    seriesstd = {}
    k = 0 
    tmp = 0
    global removed 

    if(removed == 0):
        algorithms.pop(0)
        removed = 1
    
    for algorithm in algorithms:
        series[algorithm] = []
        seriesstd[algorithm] = []
        for label in labels:
            if to_string(algorithm, label) not in data:
                #print("setting " + to_string(algorithm, label) + " to 0")
                series[algorithm].append(0)
                seriesstd[algorithm].append(0)
            else:
                series[algorithm].append(data[to_string(algorithm, label)]) 
                seriesstd[algorithm].append(datastd[to_string(algorithm, label)])

    

    n_groups = len(labels)

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.12
    opacity = 0.8
    rects = {}
     
    offset = 0

    k = 1
    
    for algorithm  in algorithms:

        alg_name = algorithm
        if "[0]" in algorithm:
            alg_name = "MESSI"
        elif "[9990]" in algorithm:
            alg_name = "MESSI-opt"
        elif "[99904]" in algorithm:
            alg_name = "MESSI-enh"
        elif "[9991]" in algorithm:
            alg_name = "MESSI-emb"
        
        elif "[9992]" in algorithm:
            alg_name = "LockFree-Full-FAI-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9993]" in algorithm:
            alg_name = "LockFree-FAI-after-help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99929]" in algorithm:
            alg_name = "LockFree-Full-FAI-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99939]" in algorithm:
            alg_name = "LockFree-FAI-after-help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[9994]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-Blocking-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9995]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-Blocking-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99949]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-Blocking-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99959]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-Blocking-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[9996]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-" 
        elif "[99966]" in algorithm:
            alg_name = "ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per subtree + hybridpqueue_ekosmas_lf_not_mixed"
        elif "[9997]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-"
        elif "[99977]" in algorithm:
            alg_name = "ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce, only after helping and per subtree + hybridpqueue_ekosmas_lf_not_mixed"   
        elif "[99969]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99979]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[99966601]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-" 
        elif "[99966602]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" 
        elif "[99966603]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-" 
        elif "[99966604]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" 
        elif "[99966605]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-" 
        elif "[99966606]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-" 
        elif "[99966607]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" 
        elif "[99966608]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" 
        elif "[999666010]" in algorithm:
            # alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI"
        elif "[999666011]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" 
        elif "[999666012]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" 
        elif "[999666013]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" 


        elif "[99977]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]


        elif "[99966691]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966692]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966693]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966694]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966695]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966696]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966697]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966698]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]




        elif "[999779]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

        elif "[999666]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-Per-Leaf-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999777]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-Per-Leaf-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99977701]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-" 
        elif "[99977702]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" 
        elif "[99977703]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-" 
        elif "[99977704]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" 
        elif "[99977705]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-No-Helping-" 
        elif "[99977706]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-No-Helping-" 
        elif "[99977707]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" 
        elif "[99977708]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" 
        elif "[999777011]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" 
        elif "[999777012]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" 
        elif "[999777013]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" 

        elif "[999777010]" in algorithm:
            # alg_name = "LockFree-after-help-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI-II"


        elif "[9996669]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-Per-Leaf-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9997779]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-Per-Leaf-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

        elif "[9998]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99981]" in algorithm:
            alg_name = "DoAllSplit"
        elif "[99981000]" in algorithm:
            alg_name = "Compare&Swap"
        elif "[9998100]" in algorithm:
            alg_name = "DoAll"
        elif "[99982]" in algorithm:
            alg_name = "Fetch&Increment"
        elif "[99983]" in algorithm:
            alg_name = "EPSimple" 
        elif "[99984]" in algorithm:
            alg_name = "EPSkipChunks"  
        elif "[99985]" in algorithm:
            alg_name = "EPOptimal "
        elif "[99986]" in algorithm:
            alg_name = "EPOptimal with chunk Size = " + algorithm.split('-')[2]
        elif "[999820]" in algorithm:
            alg_name = "EPOptimal NSB"
        elif "[999821]" in algorithm:
            alg_name = "EPOptimal NSB with chunk Size = " + algorithm.split('-')[2]
        elif "[99982000]" in algorithm:
            alg_name = "DoAllSplit NSB"            
        elif "[9998200]" in algorithm:
            alg_name = "Fetch&Increment NSB"
        elif "[999820000]" in algorithm:
            alg_name = "Compare&Swap NSB"         
        elif "[99989]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2]   + "-" + algorithm.split('-')[3]

        # rects[algorithm] = plt.errorbar(index + offset, series[algorithm], seriesstd[algorithm], bar_width,
        print(sorted_names[k])
        rects[algorithm] = plt.bar(index + offset, series[algorithm], bar_width,
            alpha=opacity,
            color = colors.get(sorted_names[k]),
            label=alg_name)
        offset += bar_width
        k = k +1
        # autolabel(rects[algorithm], ax)

    # plt.xlabel('Number of threads')
    plt.ylabel(ylabel)
    plt.title(title)
    # plt.xticks(index + bar_width, ('A', 'B', 'C', 'D'))
    plt.xticks(index + bar_width, labels)
    plt.legend(bbox_to_anchor=(1,1), loc="upper left")
     
    # plt.tight_layout()
    #plt.show()

    # plt.savefig(outdir+subdir+name+suffix_name+word+".png",bbox_inches='tight')
    print("plot_line_x_thread: saving to [" + outdir+name+".png" + "]")
    plt.savefig(outdir+name+".png",bbox_inches='tight')
    plt.close('all')
    # print "done"


def read_perf(algorithms, cr, cr_data, dataset):
    if not os.path.isdir("total"):
        os.makedirs("total")

    filename = "total/perf_"+ cr.replace(":", "-") +"["+str(dataset)+"].txt"
    with open(filename, 'w') as output:
        output.write('experiment\t\tperf counter\t\tdiff between runs\n')


        for algorithm in algorithms:
                algorithm_filename = algorithm.replace("[", "")
                algorithm_filename = algorithm_filename.replace("]", "")
                algorithm_filename = algorithm_filename.replace("-", ".")
                algorithm_filename = algorithm_filename.replace("..", ".-")
                filename = "perf/perf[" + str(dataset) + ".40." + algorithm_filename + "].out"
                with open(filename) as file:
                    user_wide = False
                    for line in file:
                        if ":u" in line:
                            user_wide = True                       
                        if (user_wide == True and ":u" in cr and ":u" not in line and cr.split(':')[0] in line) or (cr in line):
                            # if (":u" not in cr_name and ":u" not in line) or (":u" in cr_name and ":u" in line):
                            value = (line.split()[0]).replace(",", "")
                            diff = line.split()[len(line.split()) - 3]
                            key = to_string2(dataset, algorithm)
                            cr_data[key] = [int(value), diff]
                            output.write(key + '\t\t' + value + '\t\t' + diff + '\n')
                            break


def plot_line_x_thread_2(title, outdir, suffix, name, ylabel, algorithms, labels, cr, dataset):

    data = {}
    read_perf(algorithms,cr,data,dataset)

    series = {}
    for algorithm in algorithms:
        series[algorithm] = []
        for label in labels:
            if to_string2(dataset, algorithm) not in data:
                print("setting " + to_string2(dataset, algorithm) + " to 0")
                series[algorithm].append(0)
            else:
                series[algorithm].append(float(data[to_string2(dataset, algorithm)][0]))

    n_groups = len(labels)

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.12
    opacity = 0.8
    rects = {}
     
    offset = 0
    k = 1
    print(sorted_names[k])
   
    for algorithm  in algorithms:
        alg_name = algorithm
        if "[0]" in algorithm:
            alg_name = "MESSI"
        elif "[9990]" in algorithm:
            alg_name = "MESSI-opt"
        elif "[99904]" in algorithm:
            alg_name = "MESSI-enh"
        elif "[9991]" in algorithm:
            alg_name = "MESSI-emb"
        
        elif "[9992]" in algorithm:
            alg_name = "LockFree-Full-FAI-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9993]" in algorithm:
            alg_name = "LockFree-FAI-after-help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99929]" in algorithm:
            alg_name = "LockFree-Full-FAI-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99939]" in algorithm:
            alg_name = "LockFree-FAI-after-help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[9994]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-Blocking-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9995]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-Blocking-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99949]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-Blocking-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99959]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-Blocking-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[9996]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-" 
        elif "[99966]" in algorithm:
            alg_name = "ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per subtree + hybridpqueue_ekosmas_lf_not_mixed"
        elif "[9997]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-" 
        elif "[99977]" in algorithm:
            alg_name = "ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce, only after helping and per subtree + hybridpqueue_ekosmas_lf_not_mixed"   
        elif "[99969]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-NO_HELP-" 
        elif "[99979]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]      
        elif "[99966601]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-"
        elif "[99966602]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" 
        elif "[99966603]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-"
        elif "[99966604]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" 
        elif "[99966605]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-"
        elif "[99966606]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-" 
        elif "[99966607]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" 
        elif "[99966608]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" 
        elif "[999666010]" in algorithm:
            # alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI"
        elif "[999666011]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" 
        elif "[999666012]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" 
        elif "[999666013]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" 

        elif "[99977]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

        elif "[99966691]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966692]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966693]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966694]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966695]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966696]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966697]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966698]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999779]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

        elif "[999666]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-Per-Leaf-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999777]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-Per-Leaf-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99977701]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-" 
        elif "[99977702]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" 
        elif "[99977703]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-" 
        elif "[99977704]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" 
        elif "[99977705]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-No-Helping-" 
        elif "[99977706]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-No-Helping-" 
        elif "[99977707]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" 
        elif "[99977708]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" 
        elif "[999777011]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" 
        elif "[999777012]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" 
        elif "[999777013]" in algorithm:
            alg_name = "LockFree-only-after-help-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" 
        elif "[999777010]" in algorithm:
            # alg_name = "LockFree-after-help-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI-II"


        elif "[9996669]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-Per-Leaf-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9997779]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-After-Help-Per-Leaf-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

        elif "[9998]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99981]" in algorithm:
            alg_name = "DoAllSplit"
        elif "[99981000]" in algorithm:
            alg_name = "Compare&Swap"
        elif "[9998100]" in algorithm:
            alg_name = "DoAll"
        elif "[99982]" in algorithm:
            alg_name = "Fetch&Increment"
        elif "[99983]" in algorithm:
            alg_name = "EPSimple" 
        elif "[99984]" in algorithm:
            alg_name = "EPSkipChunks"  
        elif "[99985]" in algorithm:
            alg_name = "EPOptimal "
        elif "[99986]" in algorithm:
            alg_name = "EPOptimal with chunk Size = " + algorithm.split('-')[2]
        elif "[999820]" in algorithm:
            alg_name = "EPOptimal NSB"
        elif "[999821]" in algorithm:
            alg_name = "EPOptimal NSB with chunk Size = " + algorithm.split('-')[2]
        elif "[99982000]" in algorithm:
            alg_name = "DoAllSplit NSB"            
        elif "[9998200]" in algorithm:
            alg_name = "Fetch&Increment NSB"
        elif "[999820000]" in algorithm:
            alg_name = "Compare&Swap NSB"       
        elif "[99989]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-NO_HELP-" 
        elif "[99999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-NO_HELP-"


        rects[algorithm] = plt.bar(index + offset, series[algorithm], bar_width,
            alpha=opacity,
            color = colors.get(sorted_names[k]),
            label=alg_name)
        offset += bar_width
        k = k +1
        # autolabel(rects[algorithm], ax)

    # plt.xlabel('Number of threads')
    plt.ylabel(ylabel)
    plt.title(title)
    # plt.xticks(index + bar_width, ('A', 'B', 'C', 'D'))
    plt.xticks(index + bar_width, labels)
    plt.legend(bbox_to_anchor=(1,1), loc="upper left")
     
    if (cr == "cache-references:u"):
        cr_name = "cache-references-u"
    elif (cr == "cache-misses:u"):
        cr_name = "cache-misses-u"
    elif (cr == "L1-dcache-load:u"):
        cr_name = "L1-dcache-load-u"
    elif (cr == "L1-dcache-loads-misses:u"):
        cr_name = "L1-dcache-loads-misses-u"
    elif (cr == "L1-dcache-stores:u"):
        cr_name = "L1-dcache-stores-u"        
    else:
        cr_name = cr

    # plt.savefig(outdir+subdir+name+suffix_name+word+".png",bbox_inches='tight')
    print("plot_line_x_thread_2: saving to [" + outdir+name+"_"+cr_name+suffix+".png" + "]")
    plt.savefig(outdir+name+"_"+cr_name+suffix+".png",bbox_inches='tight')
    plt.close('all')
    # print "done"


def write_dict(filename, dd):
    file = open(outdir + filename, 'w') 
    file.write('experiment\t\ttotaltime\n')
    for key, value in dd.items():
        file.write(key + '\t\t' + str(value) + '\n')
    file.close() 


def new_plot_creation (ts_group_lengths, backoffs, block_lengths, algorithms, dataset, dataset_name, outdir, suffix):

    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    algorithms[0].clear
    #algorithms[0].append("[99980]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99981000]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99982]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99981]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99983]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99984]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[99985]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[999820000]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[9998200]-["+str(20000)+"]-["+str(64)+"]-[-1]")    
    #algorithms[0].append("[99982000]-["+str(20000)+"]-["+str(64)+"]-[-1]")
    #algorithms[0].append("[999820]-["+str(20000)+"]-["+str(64)+"]-[-1]")

   
    for i in block_lengths:
        for j in ts_group_lengths:
            algorithms[0].append("[99986]-["+str(i)+"]-["+str(j)+"]-[-1]")
    algorithms[0].append("[99985]-["+str(20000)+"]-["+str(64)+"]-[-1]")
 
    totaltime = {}
    totaltimeRaw = {}
    totaltimeStd = {}
    
    algorithms[0][0] = read_from_file(
        "results/results_" + "[" + str(dataset) + "]" + ".txt", 
        totaltimeRaw, 
        totaltime,
        totaltimeStd,
        labels)
    write_dict('dataset_' + str(dataset) + "_raw_values.txt", totaltime)

    for i in range(1):
        outdir2 = outdir + algorithms_labels[i] + "/"
        if not os.path.isdir(outdir2):
            os.makedirs(outdir2)

        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + suffix, 'Time (in seconds)', algorithms[i], ["total_time_in_memory"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "fill_RecBufs" + suffix, 'Time (in seconds)', algorithms[i], ["fill_SumBufs"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_tree_creation" + suffix, 'Time (in seconds)', algorithms[i], ["tree_creation"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_query_answering" + suffix, 'Time (in seconds)', algorithms[i], ["query_answering"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_duplicate_ts" + suffix, 'average duplicate ts', algorithms[i], ["duplicate_ts"], totaltime, totaltimeStd)
        
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_my_queue_fill" + suffix, 'Time (in seconds)', algorithms[i], ["my_queue_fill"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_queue_fill_help" + suffix, 'Time (in seconds)', algorithms[i], ["queue_fill_help"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_my_queue_process" + suffix, 'Time (in seconds)', algorithms[i], ["my_queue_process"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_queue_process_help" + suffix, 'Time (in seconds)', algorithms[i], ["queue_process_help"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_query_answering_counted" + suffix, 'Time (in seconds)', algorithms[i], ["query_answering_counted"], totaltime, totaltimeStd)

        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "fill_rec_buff_helping_time" + suffix, 'Time (in seconds)', algorithms[i], ["fill_sum_buff_helping_time"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "num of series helped" + suffix, 'Number of nodes (Thousands)', algorithms[i], ["num_of_series_helped"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_Total_phase1_phase2" + suffix, 'Time (in seconds)', algorithms[i], ["Total_phase1_phase2"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_stage2_helping_time" + suffix, 'Time (in seconds)', algorithms[i], ["tree_construction_helping_time"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_duplicates" + suffix, 'Number of duplicates (Thousands)', algorithms[i], ["_duplicates"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_average_stage1_helping" + suffix, 'Time (in seconds)', algorithms[i], ["average_stage1_helping_time"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_average_stage1_helping_insert" + suffix, 'Time (in seconds)', algorithms[i], ["average_stage1_helping_insert"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_average_stage1_helping_traverse" + suffix, 'Time (in seconds)', algorithms[i], ["average_stage1_helping_traverse"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_average_stage2__insert_time" + suffix, 'Time (in seconds)', algorithms[i], ["average_stage2_helping_insert"], totaltime, totaltimeStd)
        
        #for cr in perf_counters:
        #        plot_line_x_thread_2('Dataset:' + dataset_name, outdir2, suffix, 'dataset_' + str(dataset) + "_" + algorithms_labels[i], cr, algorithms[i], [cr], cr, dataset)


def init_algorithms ():
    global removed 
    global algorithms
    removed = 0
    algorithms = [0 for x in range(1)]
    for i in range(1):
        algorithms[i] = [""]


datasets = [104857600]

#block_lengths = [100, 1000, 10000, 20000, 30000, 40000, 50000]
block_lengths = [20000]

# ts_group_lengths = [1, 8, 64, 512, 2048]
ts_group_lengths = [64,128,256,512,1024,2048,4096,16384,65536,131072,262144,524288,1048576,2097152]#1310720,655360,327680,10240]#

algorithms = []
algorithms_labels = [   
                        "lockfree_Messi_Results", 
                    ]
# backoffs = [-1, 1, 10, 100, 1000, 10000, 100000, 1000000]
backoffs = [-1]
labels = [[0,"input"], [1,"total_time_in_memory"], [3, "fill_SumBufs"], [4, "tree_creation"], [5, "query_answering"], [6,"block-help-occured"], [7,"block-help-avoided"], [8,"subtree-help-occured"], [9,"subtree-help-avoided"], [11, "duplicate_ts"], [17, "my_queue_fill"], [18, "queue_fill_help"], [19, "my_queue_process"], [20, "queue_process_help"], [22, "query_answering_counted"],[23,"fill_sum_buff_helping_time"],[24,"num_of_series_helped"],[25,"Total_phase1_phase2"],[26,"_Queue_size"],[27,"tree_construction_helping_time"],[28,"_duplicates"],
    [29,"average_stage1_helping_time"],[30,"average_stage1_helping_insert"],[31,"average_stage1_helping_traverse"],[32,"average_stage2_helping_insert"],[33,"fai-stage1"],[34,"fai-stage2"],[35,"CAS+1"],[36,"CAS-1"],[37,"CAS+2"],[38,"CAS-2"]]
perf_counters = ['cache-references', 'cache-misses', 'L1-dcache-load', 'L1-dcache-loads-misses', 'L1-dcache-stores', 'cache-references:u', 'cache-misses:u', 'L1-dcache-load:u', 'L1-dcache-loads-misses:u', 'L1-dcache-stores:u']


path_ext = ""
# path_ext = "_std"

for dataset in datasets:

    dataset_name = "unknown_dataset"
    if dataset == 1048576:
        dataset_name = "1GB"
    elif dataset == 10485760:
        dataset_name = "10GB"
    elif dataset == 104857600:
        dataset_name = "100GB"

    # # Constant block_length
   # for i in block_lengths:
   #      init_algorithms()
   #      outdir = "/home1/public/geopat/grpahs/" + dataset_name + "/constant_block_length" + path_ext + "/" + str(i) + "/"
    #     new_plot_creation (ts_group_lengths, backoffs, [i], algorithms, dataset, dataset_name, outdir, "-block_" + str(i))

    # Constant ts_group_length
    for i in range(1):
        init_algorithms()
        outdir = "/home1/public/geopat/grpahs/" + dataset_name + "/constant_ts_group_length" + path_ext + "/" + "DifferentChunks" + "/"
        new_plot_creation (ts_group_lengths, backoffs, block_lengths, algorithms, dataset, dataset_name, outdir, "-group_" + str(i))

    # # Constant backoff
   # for i in backoffs:
    #     init_algorithms()
     #    outdir = "/home1/public/geopat/grpahs/" + dataset_name + "/constant_backoff/" + str(i) + "/"
      #   new_plot_creation (ts_group_lengths, [i], block_lengths, algorithms, dataset, dataset_name, outdir, "-backoff_" + str(i))

    # # Complete set of graphs
    # init_algorithms()
    # # outdir = "C:/graphs/" + dataset_name + "/complete_9990/"
    # # outdir = "C:/graphs/" + dataset_name + "/complete_9990_std/"
    # # outdir = "C:/graphs/" + dataset_name + "/complete_999666010/"
    # # outdir = "C:/graphs/" + dataset_name + "/complete_999666010_std/"
    # outdir = "C:/graphs/" + dataset_name + "/complete" + path_ext + "/"
    # # outdir = "C:/graphs/" + dataset_name + "/complete_std/"
    # new_plot_creation (ts_group_lengths, backoffs, block_lengths, algorithms, dataset, dataset_name, outdir, "")
