import os

def main():
    # Les inn variabelen fra skjermen
    variable_name = input("Skriv inn variabelnavn: ")
    variable_value = input("Skriv inn variabelverdi: ")

    # Sett miljøvariabelen
    os.environ[variable_name] = variable_value

    # Skriv ut miljøvariabelen
    print(f"Miljøvariabelen {variable_name} er nå satt til {variable_value}")

if __name__ == "__main__":
    main()