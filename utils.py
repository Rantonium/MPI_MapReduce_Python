import re
import os


def schedule_files(file_list, number_of_processes, dirr):
    total_size, file_sizes = 0, {}
    for file in file_list:
        file_sizes[file] = os.path.getsize(dirr+file)
        total_size += file_sizes[file]
    total_size = total_size / number_of_processes

    split_list = [[]]
    i = 0
    for file in file_list:
        if sum(n for _, n in split_list[i % number_of_processes]) + file_sizes[file] < total_size:
            split_list[i % number_of_processes].append((file, file_sizes[file]))
        else:
            if i < number_of_processes - 1:
                split_list.append([(file, file_sizes[file])])
            else:
                split_list[i % number_of_processes].append((file, file_sizes[file]))
            i += 1

    resulted_list = []
    for lists in split_list:
        resulted_list.append(list(n for n, _ in lists))

    k, m = divmod(len(file_list), number_of_processes)
    return resulted_list if len(resulted_list) == number_of_processes else \
        list(file_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_processes))


def extract_words(filename):
    f = open("test-files/" + filename)
    try:
        text = f.read()
    except UnicodeDecodeError:
        f = open("test-files/" + filename, encoding='iso-8859-15')
        text = f.read()
    words = list(map(lambda x: x.lower().strip(), re.findall("[\w'-.]+\w|[\w'-]+[ \t]*", text)))
    return words


def create_dictionary(words):
    word_map = {}
    for word in words:
        if word not in word_map:
            word_map[word] = 1
        else:
            word_map[word] += 1
    return word_map


def invert_dictionary(words, filename):
    word_file_map = {}
    for word in words:
        if word not in word_file_map:
            word_file_map[word] = {filename: words[word]}
        else:
            word_file_map[word][filename] += words[word]
    return word_file_map


def merge_dictionaries(dict1, dict2):
    dict3 = {**dict1, **dict2}
    for key, value in dict3.items():
        if key in dict1 and key in dict2:
            dict3[key] = {**dict1[key], **dict2[key]}
    return dict3
