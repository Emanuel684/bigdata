from typing import Any, Tuple, List
from simple_map_reduce_hdfs import SimpleMapReduceHDFS


class WordCountMapReduce(SimpleMapReduceHDFS):
    """
    Ejemplo clásico: Contador de palabras usando MapReduce.
    """
    
    def map_function(self, key: Any, value: str) -> List[Tuple[str, int]]:
        """
        Mapea una línea de texto a pares (palabra, 1).
        """
        words = value.lower().split()
        return [(word.strip('.,!?";'), 1) for word in words if word.strip('.,!?";')]
    
    def reduce_function(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        """
        Suma todos los conteos para cada palabra.
        """
        total_count = sum(values)
        return [(key, total_count)]


# Ejemplo de uso
if __name__ == "__main__":
    # Datos de ejemplo: documentos de texto
    documents = [
        (1, "la fuerza es poderosa en los jedi de la galaxia. la fuerza fluye a través de todos los seres vivos de la galaxia. los jedi usan la fuerza para proteger la paz en la galaxia y mantener el equilibrio entre la luz y la oscuridad"),
        (2, "luke skywalker es un jedi excelente con la fuerza. luke skywalker entrenó con yoda para dominar la fuerza. el jedi luke skywalker luchó contra darth vader y el emperador para salvar la galaxia del imperio"),
        (3, "el imperio es una organización de poder galáctico. el imperio controla la galaxia con mano de hierro. darth vader y el emperador lideran el imperio desde la estrella de la muerte para dominar toda la galaxia"),
        (4, "la fuerza y los jedi luchan contra el lado oscuro. los jedi protegen la galaxia del lado oscuro de la fuerza. el lado oscuro corrompe a los jedi y los convierte en sith que sirven al imperio")
    ]
    
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