# Google Maps Scraper

Un scraper multi-thread pour extraire des informations professionnelles depuis Google Maps.

## Fonctionnalités

- Scraping parallèle avec 10 workers
- Gestion du cache pour éviter les doublons
- Sauvegarde JSON par métier
- Gestion des erreurs et reprises
- Support multi-fichiers CSV

## Installation

1. Clonez le repository :
    ```
    git clone [votre-repo-url]
    cd [nom-du-dossier]
    ```

2. Installez les dépendances :
    ```
    pip install -r requirements.txt
    ```

## Structure du projet

    projet/
    ├── app.py
    ├── requirements.txt
    ├── csvLobstr/
    │   └── [vos fichiers CSV]
    └── resultats/
        └── [fichiers JSON générés]

## Utilisation

1. Placez vos fichiers CSV dans le dossier `csvLobstr/`
2. Lancez le script :
    ```
    python app.py
    ```

## Notes

- Chaque fichier CSV doit contenir une colonne 'url'
- Le nom du fichier CSV détermine le métier
- Le cache évite de retraiter les URLs déjà scrappées