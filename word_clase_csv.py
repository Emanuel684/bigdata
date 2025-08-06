from typing import Any, Tuple, List
from simple_map_reduce import SimpleMapReduce
import csv
import io
import time


class WordCountMapReduce(SimpleMapReduce):
    """
    Ejemplo clásico: Contador de palabras usando MapReduce.
    """
    
    def map_function(self, key: Any, value: str) -> List[Tuple[str, int]]:
        #usamos io.StringIO para convertir el string en un objeto file
        
        results = []
        
        # Verificar si la línea está vacía o solo contiene espacios
        if not value or not value.strip():
            return results
            
        csv_file = io.StringIO(value)
        try:
            csv_reader = csv.reader(csv_file)
            columns = next(csv_reader)
            
            # Verificar que la línea tenga al menos 7 columnas
            if len(columns) >= 7:
                print(f"{columns[0]}: {columns[6].lower()}")
                results.append((columns[6], 1))
        
        except (csv.Error, StopIteration) as e:
            print(f"Error al leer la línea: {e}")
        finally:
            csv_file.close()
        
        return results

    
    def reduce_function(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        total_count = sum(values)
        return [(key, total_count)]




# Ejemplo de uso
if __name__ == "__main__":
    # Datos de ejemplo: documentos de texto
    
    with open("data/customers-2000000.csv", "r", encoding="utf-8") as file:
        text = file.read()
        
    # Creamos chucks por linea
    chunks = text.split("\n")
    
    # eliminamos el encabezado del csv
    chunks = chunks[1:]
    
    documents = []
    
    # iteramos los chunks y generamos una lista de documentos por chunk, el indice es un consecutivo
    # Filtramos líneas vacías
    for index, chunk in enumerate(chunks):
        if chunk.strip():  # Solo agregar líneas que no estén vacías
            documents.append((index, chunk))
        
    
    print("EJEMPLO: Contador de Palabras con MapReduce")
    print("=" * 60)
    
    # Crear instancia del contador de palabras
    word_counter = WordCountMapReduce()
    
    # Ejecutar MapReduce
    start_time = time.time()
    results = word_counter.execute(documents)
    end_time = time.time()
    
    print(f"\nTiempo total de ejecución: {end_time - start_time:.2f}s")
    
    # Mostrar resultados finales
    print("RESULTADOS FINALES:")
    print("-" * 30)
    for word, count in sorted(results, key=lambda x: x[1], reverse=True):
        print(f"{word}: {count}")