# Tanker underveis

## Tanker rundt oppgaven

### 1. Onboard den offentlige datakilden til dataplattformen

Onboard dataene fra kilden til dataplattformen og dra dataen gjennom standardiserte zoner.


- Antar oppsett av dataplattform er nødvendig.
- Soner?
    - Utvikler, staging, produksjon?
    - Geografiske?
- Valg av teknologi
    - Kjøremiljø og teknologier
        - VM med CRON for schedulering av jobber.
        - Forenklet oppsett med Apache Airflow, Docker Compose og database.
        - Relasjonsdatabase vs. NoSQL-database vs. database for lokasjonsdata.
    - Airflow for å orkestrere prosessen.
    - Docker Compose for å sette opp Airflow (Kubernete)
    - Google Cloud Platform (GCP) for å kjøre Docker Compose.
- Datamodellering
    - Dataversjonering
        - [Change data capture](https://en.wikipedia.org/wiki/Change_data_capture)
    - 




### 2. Sikre datakvalitet og vedlikehold

Implementere mekanismer for å sikre at dataene er korrekte, konsistente og oppdaterte over tid.

- Rapportering og varsling ved feil.
- Sjekk datakvalitet i staging før den går til produksjon.
- Sjekk datakvalitet i produksjon.
- Typer feil
    - Feil i datagrunnlag
    - Feil i dataflyt
    - Mangler og dropout?

### 3. Automatisere prosessen 

Designe og implementere en løsning som automatisk laster og oppdaterer dataene på en pålitelig måte.

- Airflow for å orkestrere prosessen.
- Last ned data fra API som JSON.
- Konverter JSON til data som kan lagres i database.

### 4. Skape en brukervennlig datamodell

Organiser og strukturer dataene slik at de er enkle å bruke for interne brukere, som f.eks. analytikere og beslutningstakere.


### 5. Håndtere personvern og sikkerhet

Sørg for at data behandles i samsvar med gjeldende lover og regelverk.

## Fremdriftsplan

- Sett opp Airflow med Docker Compose.
    - Bruker [https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html](Apache's guide) for å sette opp Airflow med Docker Compose.
    - Setter opp en VM hos Google Cloud Platform (GCP) for å kjøre Docker Compose.
- Last ned dataene fra API-et til Kartverket.
- Sett opp databasen.
- Konverter dataene til et format som kan lagres i en database.
- Last dataene inn i en database.
- Sett opp sjekker for datakvalitet.
- Legg på det som trengs av views/transformasjoner for å gjøre dataene brukervennlige.
- Sett opp varsling ved feil.
- Personvern og sikkerhet
