import pandas as pd

def fmt_cop(val):
    """Formatea valores numéricos en formato de pesos colombianos COP ($X.XXX.XXX)."""
    if pd.isna(val) or val is None:
        return "—"
    return f"${int(val):,}".replace(",", ".")
