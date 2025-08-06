from typing import List, Tuple, Any
import json
import tempfile
from pathlib import Path

class HDFSSimulator:
    """
    Simulador simple de HDFS (Hadoop Distributed File System) usando el sistema de archivos local.
    Simula la escritura de archivos distribuidos en diferentes nodos.
    """
    
    def __init__(self, base_path: str = None):
        """
        Inicializa el simulador HDFS.
        
        Args:
            base_path: Directorio base para simular HDFS. Si es None, usa el directorio actual.
        """
        if base_path is None:
            self.base_path = Path.cwd() / "hdfs_sim"
        else:
            self.base_path = Path(base_path)
        
        self.base_path.mkdir(parents=True, exist_ok=True)
        print(f"HDFS Simulado inicializado en: {self.base_path}")
    
    def write_file(self, file_path: str, data: List[Tuple[Any, Any]], step: str = ""):
        """
        Simula la escritura de un archivo en HDFS.
        
        Args:
            file_path: Ruta del archivo en HDFS
            data: Datos a escribir (lista de tuplas)
            step: Nombre del paso para logging
        """
        full_path = self.base_path / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convertir tuplas a formato JSON para persistencia
        json_data = [{"key": key, "value": value} for key, value in data]
        
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"HDFS: Escribiendo {len(data)} registros en /{file_path} [{step}]")
        print(f"   Tamaño del archivo: {full_path.stat().st_size} bytes")
    
    def read_file(self, file_path: str) -> List[Tuple[Any, Any]]:
        """
        Simula la lectura de un archivo desde HDFS.
        
        Args:
            file_path: Ruta del archivo en HDFS
            
        Returns:
            Lista de tuplas leídas del archivo
        """
        full_path = self.base_path / file_path
        
        if not full_path.exists():
            return []
        
        with open(full_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        return [(item["key"], item["value"]) for item in json_data]
    
    def list_files(self, directory: str = "") -> List[str]:
        """
        Lista archivos en un directorio de HDFS.
        
        Args:
            directory: Directorio a listar
            
        Returns:
            Lista de archivos en el directorio
        """
        dir_path = self.base_path / directory
        if not dir_path.exists():
            return []
        
        files = []
        for item in dir_path.rglob("*"):
            if item.is_file():
                relative_path = item.relative_to(self.base_path)
                files.append(str(relative_path))
        
        return sorted(files)
    
    def cleanup(self):
        """
        Limpia el sistema de archivos simulado.
        """
        import shutil
        if self.base_path.exists():
            shutil.rmtree(self.base_path)
            print(f"HDFS Simulado limpiado: {self.base_path}")