"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import hadoop

#
# Columns:
# total_bill, tip, sex, smoker, day, time, size


# CONSULTA 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;

#Debe leer la secuencia donde la primera línea es el header, las demas son datos y agregar la columna tip_rate
def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index , (_, row) in enumerate(sequence):    #enumarate me devuelve una tupla con el indice y el valor
        if index == 0:
            result.append(
                (
                    index, 
                    row.strip() + ',tip_rate' ))  #agrego el titulo de la nueva columna
        else:            
            row_values = row.strip().split(',')  #obtengo los valores de la fila en texto (slipt: separa por comas)
            total_bill = float(row_values[0])  #obtengo el valor de total_bill
            tip = float(row_values[1])  #obtengo el valor de tip
            tip_rate = tip / total_bill 
            result.append(
                (index, row.strip() +  " " + str(tip_rate))
            )

    return result
#No se requiere reducer
def reducer_query_1(sequence):
    return sequence

#CONSULTA 2:
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner": #Filtra las filas donde time es 'Dinner'
                result.append((index, row.strip()))
    return result

def reducer_query_2(sequence):
    """Reducer"""
    return sequence

#CONSULTA 3:
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00: #Filtra las filas donde time es 'Dinner' y tip > 5.00
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence

#CONSULTA 4:
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45: #Filtra las filas donde size >= 5 o total_bill > 45
                result.append((index, row.strip()))
    return result

def reducer_query_4(sequence):
    """Reducer"""
    return sequence

#CONSULTA 5:
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#

def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result

def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    hadoop(
        mapper_fn=mapper_query_1,
        reducer_fn=reducer_query_1,
        input_folder="files/input",
        output_folder="files/query_1",
    )

    hadoop(
        mapper_fn=mapper_query_2,
        reducer_fn=reducer_query_2,
        input_folder="files/input",
        output_folder="files/query_2",
    )

    hadoop(
        mapper_fn=mapper_query_3,
        reducer_fn=reducer_query_3,
        input_folder="files/input",
        output_folder="files/query_3",
    )

    hadoop(
        mapper_fn=mapper_query_4,
        reducer_fn=reducer_query_4,
        input_folder="files/input",
        output_folder="files/query_4",
    )

    hadoop(
        mapper_fn=mapper_query_5,
        reducer_fn=reducer_query_5,
        input_folder="files/input",
        output_folder="files/query_5",
    )

if __name__ == "__main__":

    run()

