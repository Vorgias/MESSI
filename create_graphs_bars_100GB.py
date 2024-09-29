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
        # dataraw[key].remove(max(dataraw[key]))
        # dataraw[key].remove(max(dataraw[key]))
        # dataraw[key].remove(max(dataraw[key]))
        # dataraw[key].remove(max(dataraw[key]))

        # dataraw[key].remove(min(dataraw[key]))
        # dataraw[key].remove(min(dataraw[key]))
        # dataraw[key].remove(min(dataraw[key]))
        # dataraw[key].remove(min(dataraw[key]))        

        # if (max(dataraw[key])-min(dataraw[key]))/avg(dataraw[key]) > 0.1:
        #     print( str( int( 100*(max(dataraw[key])-min(dataraw[key]))/avg(dataraw[key]) ) ) + "% " + key )
        if "nodes" in key or "help-avoided" in key:
            data[key] = avg(dataraw[key])
            datastd[key] = np.std(dataraw[key])
        else:
            data[key] = avg(dataraw[key]) / 1000000.0
            datastd[key] = np.std(dataraw[key]) / 1000000.0
        # print ("adding data[" + key + "] = " + str(avg(dataraw[key])))
    return algorithm
    
    

def plot_line_x_thread(title, outdir, name, ylabel, algorithms, labels, data, datastd):

    # data = totaltime

    series = {}
    seriesstd = {}
    for algorithm in algorithms:
        series[algorithm] = []
        seriesstd[algorithm] = []
        for label in labels:
            if to_string(algorithm, label) not in data:
                print("setting " + to_string(algorithm, label) + " to 0")
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
    
    for algorithm  in algorithms:
        print(algorithm)
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
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9997]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99969]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99979]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        
        elif "[99966601]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966602]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966603]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966604]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966605]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966606]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966607]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966608]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666010]" in algorithm:
            # alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI"
        elif "[999666011]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666012]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666013]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]


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
            alg_name = "LockFree-Full Single Isax Buffer-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3] 
        elif "[99982]" in algorithm:
            alg_name = "LockFree-Full Single Isax Buffer FAD-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3] 
        elif "[9999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99989]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2]   + "-" + algorithm.split('-')[3]

        # rects[algorithm] = plt.errorbar(index + offset, series[algorithm], seriesstd[algorithm], bar_width,
        rects[algorithm] = plt.bar(index + offset, series[algorithm], bar_width,
            alpha=opacity,
            # color='b',
            label=alg_name)
        offset += bar_width
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
                filename = "perf/perf[" + str(dataset) + ".80." + algorithm_filename + "].out"
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
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[9997]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99969]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99979]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]      
        elif "[99966601]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Not-Mixed-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966602]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966603]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966604]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966605]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-No-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966606]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-No-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966607]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Single-Queue-Full-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99966608]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-1-Sorted-Array-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666010]" in algorithm:
            # alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
            alg_name = "LF-MESSI"
        elif "[999666011]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Only-After-Help-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666012]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-80-Mixed-Sorted-Array-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[999666013]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-After-Help-40-Mixed-Sorted-Array-Full-Helping-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]

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
            alg_name = "LockFree-Full Single Isax Buffer-"+ algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99982]" in algorithm:
            alg_name = "LockFree-Full Single Isax Buffer FAD-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3] 
        elif "[9999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99989]" in algorithm:
            alg_name = "LockFree-Full-FAI-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2] + "-" + algorithm.split('-')[3]
        elif "[99999]" in algorithm:
            alg_name = "LockFree-FAI-after-help-ParTree-LockFree-COW-NO_HELP-" + algorithm.split('-')[1] + "-" + algorithm.split('-')[2]   + "-" + algorithm.split('-')[3]


        rects[algorithm] = plt.bar(index + offset, series[algorithm], bar_width,
            alpha=opacity,
            #color='b',
            label=alg_name)
        offset += bar_width
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

    for i in block_lengths:
        for j in ts_group_lengths:
            algorithms[0].append("[99982]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[99981]-["+str(i)+"]-["+str(j)+"]-[-1]") 
            algorithms[0].append("[9990]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[99904]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[9992]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[9994]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[999666010]-["+str(i)+"]-["+str(j)+"]-[-1]")
            algorithms[0].append("[999777010]-["+str(i)+"]-["+str(j)+"]-[-1]")

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
        print(f"i ========= {i}")
        outdir2 = outdir + algorithms_labels[i] + "/"
        if not os.path.isdir(outdir2):
            os.makedirs(outdir2)

        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + suffix, 'Time (in seconds)', algorithms[i], ["total_time_inmemory"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_fill_RecBufs" + suffix, 'Time (in seconds)', algorithms[i], ["fill_RecBufs"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_tree_creation" + suffix, 'Time (in seconds)', algorithms[i], ["tree_creation"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_query_answering" + suffix, 'Time (in seconds)', algorithms[i], ["query_answering"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_duplicate_ts" + suffix, 'average duplicate ts', algorithms[i], ["duplicate_ts"], totaltime, totaltimeStd)
        
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_my_queue_fill" + suffix, 'Time (in seconds)', algorithms[i], ["my_queue_fill"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_queue_fill_help" + suffix, 'Time (in seconds)', algorithms[i], ["queue_fill_help"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_my_queue_process" + suffix, 'Time (in seconds)', algorithms[i], ["my_queue_process"], totaltime, totaltimeStd)
        #plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_queue_process_help" + suffix, 'Time (in seconds)', algorithms[i], ["queue_process_help"], totaltime, totaltimeStd)
        plot_line_x_thread('Dataset:' + dataset_name, outdir2, 'dataset_' + str(dataset) + "_" + algorithms_labels[i] + "_query_answering_counted" + suffix, 'Time (in seconds)', algorithms[i], ["query_answering_counted"], totaltime, totaltimeStd)

        for cr in perf_counters:
                plot_line_x_thread_2('Dataset:' + dataset_name, outdir2, suffix, 'dataset_' + str(dataset) + "_" + algorithms_labels[i], cr, algorithms[i], [cr], cr, dataset)


def init_algorithms ():
    global algorithms
    algorithms = [0 for x in range(1)]
    for i in range(1):
        # algorithms[i] = ["[9990]"]
        algorithms[i] = [""]


datasets = [104857600]
#datasets = [104857600]

#block_lengths = [100, 1000, 10000, 20000, 30000, 40000, 50000]
# block_lengths = [20000, 30000]
block_lengths = [20000]

# ts_group_lengths = [1, 8, 64, 512, 2048]
#ts_group_lengths = [1, 64, 512, 2048]
# ts_group_lengths = [1, 8, 64, 512, 2048]
ts_group_lengths = [512]

#ts_group_lengths = [512, 1]
algorithms = []
algorithms_labels = [   
                        "99981_80_Lockfree_Single_Isax_Buffer", 
                        # "999777010"
                    ]
# backoffs = [-1, 1, 10, 100, 1000, 10000, 100000, 1000000]
backoffs = [-1]
# backoffs = [-1, 1, 100, 10000, 1000000]
labels = [[0,"input"], [1,"total_time_inmemory"], [3, "fill_RecBufs"], [4, "tree_creation"], [5, "query_answering"], [6,"block-help-occured"], [7,"block-help-avoided"], [8,"subtree-help-occured"], [9,"subtree-help-avoided"], [11, "duplicate_ts"], [17, "my_queue_fill"], [18, "queue_fill_help"], [19, "my_queue_process"], [20, "queue_process_help"], [22, "query_answering_counted"]]
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
        # dataset_name = "100GB/all_values"
        # dataset_name = "100GB/all_values_999777010"
        dataset_name = "100GB"
        # dataset_name = "100GB/4_max_min_less"

    # # Constant block_length
   # for i in block_lengths:
   #      init_algorithms()
   #      outdir = "/home1/public/geopat/grpahs/" + dataset_name + "/constant_block_length" + path_ext + "/" + str(i) + "/"
    #     new_plot_creation (ts_group_lengths, backoffs, [i], algorithms, dataset, dataset_name, outdir, "-block_" + str(i))

    # Constant ts_group_length
    for i in ts_group_lengths:
        init_algorithms()
        outdir = "/home1/public/geopat/grpahs/" + dataset_name + "/constant_ts_group_length" + path_ext + "/" + str(i) + "/"
        new_plot_creation ([i], backoffs, block_lengths, algorithms, dataset, dataset_name, outdir, "-group_" + str(i))

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
