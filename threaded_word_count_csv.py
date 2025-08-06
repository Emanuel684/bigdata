from typing import Any, Tuple, List
import csv
import io
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from map_reduce_interface import MapReduceInterface


class ThreadedWordCountMapReduce(MapReduceInterface):
    """
    Implementación de MapReduce con hilos para contador de palabras.
    """
    
    def __init__(self, num_threads: int = None):
        self.num_threads = num_threads or os.cpu_count()
        self.lock = threading.Lock()
        print(f"Usando {self.num_threads} hilos (CPUs detectadas: {os.cpu_count()})")
    
    def map_function(self, key: Any, value: str) -> List[Tuple[str, int]]:
        results = []
        
        if not value or not value.strip():
            return results
            
        csv_file = io.StringIO(value)
        try:
            csv_reader = csv.reader(csv_file)
            columns = next(csv_reader)
            
            if len(columns) >= 7:
                with self.lock:
                    print(f"{columns[0]}: {columns[6].lower()}")
                results.append((columns[6], 1))
        
        except (csv.Error, StopIteration) as e:
            with self.lock:
                print(f"Error al leer la línea: {e}")
        finally:
            csv_file.close()
        
        return results

    def reduce_function(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        total_count = sum(values)
        return [(key, total_count)]

    def _threaded_map_phase(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        start_time = time.time()
        print("Iniciando fase MAP con hilos...")
        
        intermediate_results = []
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_data = {executor.submit(self.map_function, key, value): (key, value) 
                             for key, value in input_data}
            
            for future in as_completed(future_to_data):
                key, value = future_to_data[future]
                try:
                    mapped = future.result()
                    intermediate_results.extend(mapped)
                except Exception as exc:
                    print(f'MAP falló para ({key}, {value}): {exc}')
        
        end_time = time.time()
        print(f"Fase MAP completada en {end_time - start_time:.2f}s. {len(intermediate_results)} pares intermedios generados.\n")
        return intermediate_results

    def _shuffle_phase(self, intermediate_data: List[Tuple[Any, Any]]) -> dict:
        start_time = time.time()
        print("Iniciando fase SHUFFLE...")
        
        grouped_data = defaultdict(list)
        
        for key, value in intermediate_data:
            grouped_data[key].append(value)
            
        sorted_groups = dict(sorted(grouped_data.items()))
        
        for key, values in sorted_groups.items():
            print(f"   SHUFFLE: {key} -> {values}")
            
        end_time = time.time()
        print(f"Fase SHUFFLE completada en {end_time - start_time:.2f}s. {len(sorted_groups)} grupos creados.\n")
        return sorted_groups

    def _threaded_reduce_phase(self, grouped_data: dict) -> List[Tuple[Any, Any]]:
        start_time = time.time()
        print("Iniciando fase REDUCE con hilos...")
        
        final_results = []
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_key = {executor.submit(self.reduce_function, key, values): key 
                            for key, values in grouped_data.items()}
            
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    reduced = future.result()
                    final_results.extend(reduced)
                except Exception as exc:
                    print(f'REDUCE falló para {key}: {exc}')
        
        end_time = time.time()
        print(f"Fase REDUCE completada en {end_time - start_time:.2f}s. {len(final_results)} resultados finales.\n")
        return final_results

    def execute(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        total_start = time.time()
        print("Iniciando proceso MapReduce con hilos")
        print("=" * 50)
        
        # Fase 1: Map con hilos
        intermediate_data = self._threaded_map_phase(input_data)
        
        # Fase 2: Shuffle/Sort
        grouped_data = self._shuffle_phase(intermediate_data)
        
        # Fase 3: Reduce con hilos
        final_results = self._threaded_reduce_phase(grouped_data)
        
        total_end = time.time()
        print(f"Proceso MapReduce con hilos completado en {total_end - total_start:.2f}s!")
        print("=" * 50)
        
        return final_results


# Ejemplo de uso
if __name__ == "__main__":
    with open("data/customers-2000000.csv", "r", encoding="utf-8") as file:
        text = file.read()
        
    chunks = text.split("\n")[1:]  # Eliminar encabezado
    
    documents = [(index, chunk) for index, chunk in enumerate(chunks) if chunk.strip()]
        
    print("EJEMPLO: Contador de Palabras con MapReduce usando Hilos")
    print("=" * 60)
    
    # Crear instancia usando todos los CPUs disponibles
    word_counter = ThreadedWordCountMapReduce()
    
    # Ejecutar MapReduce
    results = word_counter.execute(documents)
    
    # Mostrar resultados finales
    print("\nRESULTADOS FINALES:")
    print("-" * 30)
    for word, count in sorted(results, key=lambda x: x[1], reverse=True):
        print(f"{word}: {count}")