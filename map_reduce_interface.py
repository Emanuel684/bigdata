from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class MapReduceInterface(ABC):
    """
    Interfaz abstracta que define el contrato para implementaciones de MapReduce.
    Los estudiantes deben heredar de esta clase e implementar los métodos abstractos.
    """

    @abstractmethod
    def map_function(self, key: Any, value: Any) -> List[Tuple[Any, Any]]:
        """
        Función de mapeo que transforma un par clave-valor de entrada
        en una lista de pares clave-valor intermedios.

        Args:
            key: Clave de entrada
            value: Valor de entrada

        Returns:
            Lista de tuplas (clave_intermedia, valor_intermedio)
        """
        pass

    @abstractmethod
    def reduce_function(self, key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
        """
        Función de reducción que combina todos los valores asociados
        con una clave intermedia específica.

        Args:
            key: Clave intermedia
            values: Lista de valores asociados con la clave

        Returns:
            Lista de tuplas (clave_final, valor_final)
        """
        pass
