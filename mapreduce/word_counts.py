import csv
import io
from typing import Any, List, Tuple

from simple_map_reduce import SimpleMapReduce


class WordCountMapReduce(SimpleMapReduce):
    """
    Ejemplo clásico: Contador de palabras usando MapReduce.
    """

    def map_function(self, key: Any, value: str) -> List[Tuple[str, int]]:
        # Usamos un diccionario para contar palabras
        csv_file = io.StringIO(value)
        results = []
        try:
            reader = csv.reader(csv_file)
            columns = next(reader)  # Leer la primera fila como encabezados
            print("Encabezados:", columns)
            results.append(
                (columns[6], 1)
            )  # Guardar el número de columnas como un ejemplo
        except Exception as e:
            print("Error al leer el CSV:", e)

        return results

        # pass

    def reduce_function(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        total_count = sum(values)
        return [(key, total_count)]


# Ejemplo de uso
if __name__ == "__main__":
    # with open("data/La_divina_comedia-Dante_Alighieri.txt", "r", encoding="utf-8") as f:
    #     file = f.read()

    # Datos de ejemplo: documentos de texto
    # documents = [
    #     ("comedia", file),
    # ]

    # documents = [
    #     (1,
    #      "la fuerza es poderosa en los jedi de la galaxia. la fuerza fluye a través de todos los seres vivos de la galaxia. los jedi usan la fuerza para proteger la paz en la galaxia y mantener el equilibrio entre la luz y la oscuridad"),
    #     (2,
    #      "luke skywalker es un jedi excelente con la fuerza. luke skywalker entrenó con yoda para dominar la fuerza. el jedi luke skywalker luchó contra darth vader y el emperador para salvar la galaxia del imperio"),
    #     (3,
    #      "el imperio es una organización de poder galáctico. el imperio controla la galaxia con mano de hierro. darth vader y el emperador lideran el imperio desde la estrella de la muerte para dominar toda la galaxia"),
    #     (4,
    #      "la fuerza y los jedi luchan contra el lado oscuro. los jedi protegen la galaxia del lado oscuro de la fuerza. el lado oscuro corrompe a los jedi y los convierte en sith que sirven al imperio")
    # ]

    with open("data/customers-2000000.csv", "r", encoding="utf-8") as f:
        file = f.read()

    # Datos de ejemplo: documentos de texto
    documents = []

    # Creamos chunks por line
    chunks = file.split("\n")

    chunks = chunks[1::]  # Limitar a los primeros 1000 para pruebas

    # Iteramos sobre los chunks y los agregamos a la lista de documentos
    # Cada chunk es una tupla (índice, contenido del chunk)
    for index, chunk in enumerate(chunks):
        documents.append((index, chunk))

    print("EJEMPLO: Contador de Palabras con MapReduce")
    print("=" * 60)

    # Crear instancia del contador de palabras
    word_counter = WordCountMapReduce()

    # Ejecutar MapReduce
    results = word_counter.execute(documents)

    # Mostrar resultados finales
    print("\nRESULTADOS FINALES:")
    print("-" * 30)
    for word, count in sorted(results, key=lambda x: x[1], reverse=True):
        print(f"{word}: {count}")

    top_10 = sorted(results, key=lambda x: x[1], reverse=True)[:10]
    import csv

    with open("top_10_words.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["word", "count"])
        writer.writerows(top_10)
