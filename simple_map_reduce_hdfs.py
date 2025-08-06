from collections import defaultdict
from typing import Dict, List, Tuple, Any

from hdfs_simulator import HDFSSimulator
from map_reduce_interface import MapReduceInterface


class SimpleMapReduceHDFS(MapReduceInterface):
    """
    Implementación simple de MapReduce en Python puro.
    Esta clase demuestra los conceptos básicos sin usar Hadoop.
    """
    
    def __init__(self):
        """
        Inicializa el framework MapReduce con simulador HDFS.
        """
        self.hdfs = HDFSSimulator()
        self.job_id = f"job_{hash(str(id(self))) % 10000:04d}"
        print(f"Job ID asignado: {self.job_id}")
    
    def map_function(self, key: Any, value: Any) -> List[Tuple[Any, Any]]:
        """
        Implementación por defecto de la función map.
        Los estudiantes deben sobrescribir este método.
        """
        return [(key, value)]
    
    def reduce_function(self, key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
        """
        Implementación por defecto de la función reduce.
        Los estudiantes deben sobrescribir este método.
        """
        return [(key, values)]
    
    def _map_phase(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        """
        Ejecuta la fase de mapeo sobre los datos de entrada.
        Simula la escritura de resultados intermedios en HDFS.
        
        Args:
            input_data: Lista de pares (clave, valor) de entrada
            
        Returns:
            Lista de pares (clave, valor) intermedios
        """
        print("Iniciando fase MAP...")
        
        # 1. Escribir datos de entrada en HDFS
        input_path = f"jobs/{self.job_id}/input/input_data.json"
        self.hdfs.write_file(input_path, input_data, "INPUT")
        
        intermediate_results = []
        
        # 2. Procesar cada registro y simular escritura por chunks
        chunk_size = 2  # Simular escritura en chunks pequeños
        chunk_num = 0
        
        for i in range(0, len(input_data), chunk_size):
            chunk = input_data[i:i + chunk_size]
            chunk_results = []
            
            for key, value in chunk:
                mapped = self.map_function(key, value)
                chunk_results.extend(mapped)
                intermediate_results.extend(mapped)
                print(f"   MAP: ({key}, {value}) -> {mapped}")
            
            # 3. Escribir chunk de resultados intermedios en HDFS
            if chunk_results:
                chunk_path = f"jobs/{self.job_id}/intermediate/map_output_chunk_{chunk_num:03d}.json"
                self.hdfs.write_file(chunk_path, chunk_results, f"MAP_CHUNK_{chunk_num}")
                chunk_num += 1
        
        # 4. Escribir archivo consolidado de resultados MAP
        consolidated_path = f"jobs/{self.job_id}/intermediate/map_output_consolidated.json"
        self.hdfs.write_file(consolidated_path, intermediate_results, "MAP_CONSOLIDATED")
        
        print(f"Fase MAP completada. {len(intermediate_results)} pares intermedios generados.")
        print(f"Archivos MAP creados: {chunk_num} chunks + 1 consolidado\n")
        
        return intermediate_results
    
    def _shuffle_phase(self, intermediate_data: List[Tuple[Any, Any]]) -> Dict[Any, List[Any]]:
        """
        Ejecuta la fase de shuffle/sort, agrupando valores por clave.
        Simula la escritura de datos agrupados en HDFS por particiones.
        
        Args:
            intermediate_data: Lista de pares (clave, valor) intermedios
            
        Returns:
            Diccionario con claves como keys y listas de valores como values
        """
        print("Iniciando fase SHUFFLE...")
        
        # 1. Leer datos intermedios desde HDFS (simulación de lectura distributiva)
        consolidated_path = f"jobs/{self.job_id}/intermediate/map_output_consolidated.json"
        print(f"Leyendo datos intermedios desde HDFS: /{consolidated_path}")
        
        grouped_data = defaultdict(list)
        
        # 2. Agrupar datos por clave
        for key, value in intermediate_data:
            grouped_data[key].append(value)
            
        # 3. Ordenar las claves para consistencia
        sorted_groups = dict(sorted(grouped_data.items()))
        
        # 4. Simular escritura de particiones en HDFS
        # En Hadoop real, cada reducer recibe una partición de claves
        partition_size = max(1, len(sorted_groups) // 3)  # Simular 3 reducers
        partition_num = 0
        current_partition = {}
        
        for i, (key, values) in enumerate(sorted_groups.items()):
            current_partition[key] = values
            print(f"   SHUFFLE: {key} -> {values}")
            
            # Escribir partición cuando alcance el tamaño límite
            if len(current_partition) >= partition_size or i == len(sorted_groups) - 1:
                partition_path = f"jobs/{self.job_id}/partitions/partition_{partition_num:03d}.json"
                partition_data = [(k, v) for k, v_list in current_partition.items() for v in v_list]
                self.hdfs.write_file(partition_path, partition_data, f"PARTITION_{partition_num}")
                
                partition_num += 1
                current_partition = {}
        
        # 5. Escribir archivo consolidado de grupos
        grouped_consolidated_path = f"jobs/{self.job_id}/shuffle/grouped_data.json"
        grouped_flat = [(k, v_list) for k, v_list in sorted_groups.items()]
        self.hdfs.write_file(grouped_consolidated_path, grouped_flat, "SHUFFLE_CONSOLIDATED")
        
        print(f"Fase SHUFFLE completada. {len(sorted_groups)} grupos creados.")
        print(f"Particiones creadas: {partition_num}\n")
        
        return sorted_groups
    
    def _reduce_phase(self, grouped_data: Dict[Any, List[Any]]) -> List[Tuple[Any, Any]]:
        """
        Ejecuta la fase de reducción sobre los datos agrupados.
        Simula la escritura de resultados finales en HDFS.
        
        Args:
            grouped_data: Diccionario con datos agrupados por clave
            
        Returns:
            Lista de pares (clave, valor) finales
        """
        print("Iniciando fase REDUCE...")
        
        final_results = []
        
        # 1. Simular procesamiento por reducer (cada uno procesa ciertas claves)
        reducer_outputs = []
        reducer_num = 0
        
        # Dividir trabajo entre múltiples reducers simulados
        keys = list(grouped_data.keys())
        reducer_batch_size = max(1, len(keys) // 2)  # Simular 2 reducers
        
        for i in range(0, len(keys), reducer_batch_size):
            reducer_batch = keys[i:i + reducer_batch_size]
            reducer_results = []
            
            print(f"   REDUCER {reducer_num} procesando claves: {reducer_batch}")
            
            for key in reducer_batch:
                values = grouped_data[key]
                reduced = self.reduce_function(key, values)
                reducer_results.extend(reduced)
                final_results.extend(reduced)
                print(f"      REDUCE: {key}, {values} -> {reduced}")
            
            # 2. Escribir salida de cada reducer en HDFS
            if reducer_results:
                reducer_output_path = f"jobs/{self.job_id}/output/reducer_{reducer_num:03d}.json"
                self.hdfs.write_file(reducer_output_path, reducer_results, f"REDUCER_{reducer_num}")
                reducer_outputs.append(f"reducer_{reducer_num:03d}.json")
            
            reducer_num += 1
        
        # 3. Escribir resultado final consolidado
        final_output_path = f"jobs/{self.job_id}/output/final_output.json"
        self.hdfs.write_file(final_output_path, final_results, "FINAL_OUTPUT")
        
        # 4. Crear archivo de metadatos del job
        metadata = {
            "job_id": self.job_id,
            "total_results": len(final_results),
            "reducers_used": reducer_num,
            "reducer_outputs": reducer_outputs,
            "status": "COMPLETED"
        }
        metadata_path = f"jobs/{self.job_id}/job_metadata.json"
        metadata_tuples = [(k, v) for k, v in metadata.items()]
        self.hdfs.write_file(metadata_path, metadata_tuples, "METADATA")
        
        print(f"Fase REDUCE completada. {len(final_results)} resultados finales.")
        print(f"Archivos de salida: {len(reducer_outputs)} reducers + 1 consolidado\n")
        
        return final_results
    
    def execute(self, input_data: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        """
        Ejecuta el pipeline completo de MapReduce con simulación HDFS.
        
        Args:
            input_data: Datos de entrada como lista de pares (clave, valor)
            
        Returns:
            Resultados finales como lista de pares (clave, valor)
        """
        print("Iniciando proceso MapReduce con simulación HDFS")
        print("=" * 60)
        
        # Fase 1: Map
        intermediate_data = self._map_phase(input_data)
        
        # Fase 2: Shuffle/Sort
        grouped_data = self._shuffle_phase(intermediate_data)
        
        # Fase 3: Reduce
        final_results = self._reduce_phase(grouped_data)
        
        # Mostrar estructura de archivos HDFS
        self._show_hdfs_structure()
        
        print("Proceso MapReduce completado!")
        print("=" * 60)
        
        return final_results
    
    def _show_hdfs_structure(self):
        """
        Muestra la estructura de archivos creada en HDFS durante el job.
        """
        print("ESTRUCTURA DE ARCHIVOS HDFS:")
        print("-" * 40)
        
        files = self.hdfs.list_files()
        for file_path in files:
            if self.job_id in file_path:
                file_size = (self.hdfs.base_path / file_path).stat().st_size
                print(f"   /{file_path} ({file_size} bytes)")
        
        print()
    
    def cleanup_hdfs(self):
        """
        Limpia los archivos HDFS del job.
        """
        self.hdfs.cleanup()
