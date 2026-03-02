import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """Retorna una instancia de logger configurada profesionalmente."""
    logger = logging.getLogger(name)
    
    # Previene la adición múltiple de handlers
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # StreamHandler para imprimir logs en consola (y que queden en GitHub Actions)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        
    return logger
