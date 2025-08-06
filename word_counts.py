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

    with open("top_10_country.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country", "count"])
        writer.writerows(top_10)
