import pandas as pd
import glob


# Questa funzione estrae i dati dai file csv, e li ritorna come un unico Dataframe
def get_data():
    csv_files = glob.glob('Dataset/*.csv')

    dfs = []
    for csv_file in csv_files:
        dfs.append(pd.read_csv(csv_file))

    return pd.concat(dfs, ignore_index = True)



# Questa funzione calcola l'event time di ciascuna tupla, aggiungendo una colonna al Dataframe. Tale colonna sarà utilizzata
# per ordinare le tuple e calcolare il fattore di accelerazione, ma NON VIENE INVIATA all'interno dello stream di dati
def compute_event_time(data):

    dep_time = data['CRS_DEP_TIME']

    hours = dep_time // 100
    minutes = dep_time % 100

    hours[hours == 24] = 0

    date = pd.to_datetime({
        'year': data['YEAR'],
        'month': data['MONTH'],
        'day': data['DAY_OF_MONTH'],
        'hour': hours,
        'minute': minutes
    }, errors = 'coerce', utc = True)

    return date.astype("datetime64[s, UTC]").astype("int64")


# Questa funzione calcola il fattore di speedup. La funzione riceve in input la durata totale dello stream, in minuti,
# e la colonna degli event times
def compute_speedup_factor(target_duration, event_times):

    min_event_time = event_times.min()
    max_event_time = event_times.max()

    interval = max_event_time - min_event_time

    # Converto in secondi perchè gli event time sono espressi in secondi
    target_duration = target_duration * 60

    return interval / target_duration
