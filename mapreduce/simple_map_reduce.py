from collections import defaultdict
from typing import Any, Dict, List, Tuple

from map_reduce_interface import MapReduceInterface


class SimpleMapReduce(MapReduceInterface):
    """
    Implementación simple de MapReduce en Python puro.
    Esta clase demuestra los conceptos básicos sin usar Hadoop.
    """

    def __init__(self):
        """
        Inicializa el framework MapReduce.
        """
        pass

    def map_function(self, key: Any, value: Any) -> List[Tuple[Any, Any]]:
        """
        Implementación por defecto de la función map.
        Los estudiantes deben sobrescribir este método.
        """
        return [(key, value)]

        # print("key:", key, "value:", value)
        # words = value.lower().split()
        # result = []
        # for word in words:
        #     # Emitir cada palabra con un conteo de 1
        #     key = word.strip('.,!?";')  # Limpiar caracteres especiales
        #     if key:
        #         # Asegurarse de que la clave no esté vacía
        #         print("word:", key)
        #         # Emitir la palabra como clave y 1 como valor
        #         result.append((key, 1))
        # return result

    def reduce_function(self, key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
        """
        Implementación por defecto de la función reduce.
        Los estudiantes deben sobrescribir este método.
        """
        suma = sum(values)
        return [(key, suma)]

    def _map_phase(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        """
        Ejecuta la fase de mapeo sobre los datos de entrada.

        Args:
            input_data: Lista de pares (clave, valor) de entrada

        Returns:
            Lista de pares (clave, valor) intermedios
        """
        print("Iniciando fase MAP...")

        intermediate_results = []

        # Procesamiento secuencial
        for key, value in input_data:
            print(f"Procesando entrada: ({key}, {value})")

            # mapped = self.map_function(key=key, value=value)
            print("key:", key, "value:", value)
            words = value.lower().split()
            mapped = []
            for word in words:
                # Emitir cada palabra con un conteo de 1
                key = word.strip('.,!?";')  # Limpiar caracteres especiales
                if key:
                    # Asegurarse de que la clave no esté vacía
                    print("word:", key)
                    # Emitir la palabra como clave y 1 como valor
                    mapped.append((key, 1))

            print("mapped:", mapped)
            intermediate_results.extend(mapped)
            print(f"   MAP: ({key}, {value}) -> {mapped}")

        print(
            f"Fase MAP completada. {len(intermediate_results)} pares intermedios generados.\n"
        )
        return intermediate_results

    def _shuffle_phase(
        self, intermediate_data: List[Tuple[Any, Any]]
    ) -> Dict[Any, List[Any]]:
        """
        Ejecuta la fase de shuffle/sort, agrupando valores por clave.

        Args:
            intermediate_data: Lista de pares (clave, valor) intermedios

        Returns:
            Diccionario con claves como keys y listas de valores como values
        """
        print("Iniciando fase SHUFFLE...")

        grouped_data = defaultdict(list)

        for key, value in intermediate_data:
            grouped_data[key].append(value)

        # Ordenar las claves para consistencia
        sorted_groups = dict(sorted(grouped_data.items()))

        for key, values in sorted_groups.items():
            print(f"   SHUFFLE: {key} -> {values}")

        print(f"Fase SHUFFLE completada. {len(sorted_groups)} grupos creados.\n")
        return sorted_groups

    def _reduce_phase(
        self, grouped_data: Dict[Any, List[Any]]
    ) -> List[Tuple[Any, Any]]:
        """
        Ejecuta la fase de reducción sobre los datos agrupados.

        Args:
            grouped_data: Diccionario con datos agrupados por clave

        Returns:
            Lista de pares (clave, valor) finales
        """
        print("Iniciando fase REDUCE...")

        final_results = []

        for key, values in grouped_data.items():
            # reduced = self.reduce_function(key, values)
            reduced = [(key, sum(values))]
            final_results.extend(reduced)
            print(f"   REDUCE: {key}, {sum(values)} -> {reduced}")

        print(f"Fase REDUCE completada. {len(final_results)} resultados finales.\n")
        return final_results

    def execute(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        """
        Ejecuta el pipeline completo de MapReduce.

        Args:
            input_data: Datos de entrada como lista de pares (clave, valor)

        Returns:
            Resultados finales como lista de pares (clave, valor)
        """
        print("Iniciando proceso MapReduce")
        print("=" * 50)

        # Fase 1: Map
        intermediate_data = self._map_phase(input_data)

        # Fase 2: Shuffle/Sort
        grouped_data = self._shuffle_phase(intermediate_data)

        # Fase 3: Reduce
        final_results = self._reduce_phase(grouped_data)

        print("Proceso MapReduce completado!")
        print("=" * 50)

        return final_results
