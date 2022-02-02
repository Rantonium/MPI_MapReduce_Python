from mpi4py import MPI
from utils import *
import json
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
result_directory_name = sys.argv[2]

# Master
# mpiexec -n 9 python map_reduce.py test-files result-dir
if rank == 0:
    # wordload balance distribution
    file_scheduler_result = schedule_files(os.listdir(sys.argv[1] + "/"), size - 3, sys.argv[1] + "/")

    # start, send file names to mappers
    for index, file_list in enumerate(file_scheduler_result):
        comm.send(file_list, dest=index + 3, tag=99)
    # receive file names from mappers, send to reducers
    for i in range(2, size - 1):
        data = comm.recv(source=MPI.ANY_SOURCE, tag=96)
        comm.send(data, int(data.split('.')[0]) % 2 + 1, tag=98)
    # receive from reducers, done flags
    for i in range(0, 2):
        comm.recv(source=MPI.ANY_SOURCE, tag=97)

    print("Processing done")

# Reducer
elif rank == 1 or rank == 2:
    # Reduce phase
    final_result = dict()
    for i in range(0, int((size - 1) / 2) - 1):
        data = comm.recv(source=MPI.ANY_SOURCE, tag=98)
        f = open('combining_results/' + data)
        final_result = merge_dictionaries(final_result, json.loads(f.read()))
    if size % 2 == 0 and rank == 2:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=98)
        f = open('combining_results/' + data)
        final_result = merge_dictionaries(final_result, json.loads(f.read()))
    # noinspection PyBroadException
    try:
        os.mkdir(os.path.abspath(os.getcwd()) + "/" + result_directory_name)
    except:
        pass
    with open(result_directory_name + '/final_result' + str(rank) + '.txt', 'w') as final_file:
        final_file.write(json.dumps(final_result))

    # Notify ROOT that Reducing phase is done
    comm.send(1, 0, tag=97)

# Mapper
elif rank != 0:
    # noinspection PyBroadException
    try:
        os.mkdir(os.path.abspath(os.getcwd()) + "/mapping_results")
        os.mkdir(os.path.abspath(os.getcwd()) + "/combining_results")
    except:
        pass
    # Mapping phase
    files = comm.recv(source=0, tag=99)
    for file in files:
        words = extract_words(file)
        word_map = create_dictionary(words)
        # Scriem in fisier lista de cuvinte, deja inversata sa fie de forma <cuvant: {fisier: numar_aparitii}>
        with open('mapping_results/' + file.split('.')[0] + '_result.txt', 'w') as output_file:
            output_file.write(json.dumps(word_map))

    # Combining phase
    result = dict()
    for file in files:
        f = open('mapping_results/' + file.split('.')[0] + '_result.txt')
        result = merge_dictionaries(result, invert_dictionary(json.loads(f.read()), file))
    with open('combining_results/' + str(rank) + '.txt', 'w') as result_file:
        result_file.write(json.dumps(result))

    # Notify ROOT that Mapping is done
    comm.send(str(rank) + '.txt', dest=0, tag=96)
