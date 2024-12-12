from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import random
import csv
from bs4 import BeautifulSoup
import time as sleep_module
import json
import os
from multiprocessing import Pool, cpu_count, Lock, Manager
from functools import partial
import glob
import hashlib

class GoogleMapsScraper:
    def __init__(self):
        # Configuration du webdriver
        options = webdriver.ChromeOptions()
        #options.add_argument('--headless')  # Mode sans interface graphique
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        self.driver = webdriver.Chrome(options=options)
        self.driver.get("https://www.google.fr")
        self.driver.add_cookie({
            'name': 'CONSENT',
            'value': 'YES+cb',
            'domain': '.google.fr'
        })
        self.wait = WebDriverWait(self.driver, 3)

    def scrape_listing(self, url, metier, csv_file, lock):
        try:
            self.driver.get(url)
            sleep_module.sleep(1)
            
            # Gérer le deuxième consentement de cookies s'il est présent
            try:
                cookie_button = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.UywwFc-RLmnJb"))
                )
                self.driver.execute_script("arguments[0].click();", cookie_button)
                sleep_module.sleep(0.3)  # Court délai après le clic
            except (TimeoutException, NoSuchElementException):
                pass  # Continuer si le bouton n'est pas trouvé
            
            print("Page title:", self.driver.title)
            print("Current URL:", self.driver.current_url)
            
            # Attendre que la liste des résultats soit chargée
            results_container = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )
            
            # Faire défiler la liste des résultats jusqu'à ce que tous soient chargés
            last_height = self.driver.execute_script(
                "return arguments[0].scrollHeight", results_container
            )
            
            while True:
                # Faire défiler vers le bas
                self.driver.execute_script(
                    "arguments[0].scrollTo(0, arguments[0].scrollHeight);", 
                    results_container
                )
                sleep_module.sleep(2)
                
                # Calculer la nouvelle hauteur
                new_height = self.driver.execute_script(
                    "return arguments[0].scrollHeight", 
                    results_container
                )
                
                # Vérifier si on a atteint le bas
                if new_height == last_height:
                    print("Fin du scroll - tous les résultats sont chargés")
                    break
                    
                print(f"Scrolling... ({new_height}px)")
                last_height = new_height
                
            # Maintenant que tous les résultats sont chargés, les récupérer
            results = self.wait.until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "hfpxzc"))
            )
            print(f"Nombre total de résultats trouvés : {len(results)}")
            
            data = []
            
            def get_fresh_results():
                """Récupérer une nouvelle liste de résultats"""
                return self.wait.until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "hfpxzc"))
                )
            
            for i in range(len(results)):
                try:
                    # Récupérer une nouvelle liste de résultats à chaque itération
                    fresh_results = get_fresh_results()
                    
                    if i >= len(fresh_results):
                        print(f"Index {i} hors limites, arrêt du scraping")
                        break
                    
                    current_result = fresh_results[i]
                    
                    # Vérifier si l'élément est visible
                    if not self.is_element_visible(current_result):
                        print(f"Élément {i+1} non visible, tentative de scroll")
                        self.scroll_to_element(current_result)
                        sleep_module.sleep(0.5)
                        
                        # Récupérer à nouveau l'élément après le scroll
                        fresh_results = get_fresh_results()
                        if i < len(fresh_results):
                            current_result = fresh_results[i]
                        else:
                            continue
                    
                    # Cliquer sur le résultat avec retry
                    retry_count = 3
                    for attempt in range(retry_count):
                        try:
                            self.driver.execute_script("arguments[0].click();", current_result)
                            sleep_module.sleep(0.8)
                            break
                        except Exception as e:
                            if attempt == retry_count - 1:
                                raise e
                            self.driver.refresh()
                            sleep_module.sleep(1)
                            fresh_results = get_fresh_results()
                            if i < len(fresh_results):
                                current_result = fresh_results[i]
                    
                    # Attendre que le panneau principal soit chargé
                    self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main']"))
                    )
                    
                    # Attendre que les détails soient chargés
                    self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.rogA2c"))
                    )
                    
                    # Extraire les informations avec des sélecteurs plus précis
                    try:
                        name = self.driver.find_element(By.CSS_SELECTOR, "h1.fontHeadlineLarge").text
                    except NoSuchElementException:
                        try:
                            name = self.driver.find_element(By.CSS_SELECTOR, "h1.DUwDvf").text
                        except NoSuchElementException:
                            name = "Non disponible"
                    
                    try:
                        address_element = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id='address'] div.Io6YTe"))
                        )
                        address = address_element.text
                    except (NoSuchElementException, TimeoutException):
                        address = "Non disponible"
                    
                    try:
                        phone_element = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "button[data-item-id^='phone:tel:'] div.Io6YTe"))
                        )
                        phone = phone_element.text
                    except (NoSuchElementException, TimeoutException):
                        phone = "Non disponible"
                    
                    try:
                        website_element = self.wait.until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-item-id='authority']"))
                        )
                        website = website_element.get_attribute('href')
                    except (NoSuchElementException, TimeoutException):
                        website = "Non disponible"

                    # Extraire les horaires
                    hours = {}
                    schedule_rows = self.driver.find_elements(By.CSS_SELECTOR, "table.eK4R0e tr")
                    for row in schedule_rows:
                        day = row.find_element(By.CLASS_NAME, "ylH6lf").text
                        opening_hours = row.find_element(By.CLASS_NAME, "mxowUb").text
                        hours[day] = opening_hours

                    data.append({
                        "name": name,
                        "address": address,
                        "phone": phone,
                        "website": website,
                        "metier": metier
                    })
                    
                    result_data = {
                        "name": name,
                        "address": address,
                        "phone": phone,
                        "website": website,
                        "hours": hours,
                        "metier": metier
                    }
                    
                    # Utiliser la version thread-safe de la sauvegarde
                    save_to_json_safe(result_data, csv_file, lock)
                    
                    print(f"\nRésultat {i+1}/{len(results)}:")
                    print(f"Nom: {name}")
                    print(f"Adresse: {address}")
                    print(f"Téléphone: {phone}")
                    print(f"Site web: {website}")
                    print(f"Métier: {metier}")
                    
                except Exception as e:
                    print(f"Erreur lors du scraping du résultat {i+1}: {str(e)}")
                    self.driver.refresh()
                    sleep_module.sleep(1)
                    continue
                    
            return data
            
        except Exception as e:
            print(f"Erreur générale: {str(e)}")
            return []

    def extract_listing_info(self):
        """Extraire les informations d'une fiche professionnelle"""
        info = {}
        
        try:
            # Créer l'objet soup
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Nom
            info['nom'] = soup.select_one("h1.DUwDvf").text if soup.select_one("h1.DUwDvf") else None
            
            # Adresse
            info['adresse'] = self.safe_get_text("button[data-item-id='address']")
            
            # Téléphone
            info['telephone'] = self.safe_get_text("button[data-item-id='phone:tel:']")
            
            # Site web
            info['site_web'] = self.safe_get_text("a[data-item-id='authority']")
            
            # Note moyenne
            info['note'] = self.safe_get_text("div.fontDisplayLarge")
            
            # Nombre d'avis
            info['nb_avis'] = self.safe_get_text("button[aria-label*='avis']")
            
            # Horaires
            info['horaires'] = self.get_hours()

        except Exception as e:
            print(f"Erreur lors de l'extraction des détails : {str(e)}")

        return info

    def safe_get_text(self, selector):
        """Obtenir le texte d'un élément de manière sécurisée"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.text
        except NoSuchElementException:
            return None

    def get_hours(self):
        """Extraire les horaires d'ouverture"""
        try:
            # Cliquer sur le bouton des horaires
            hours_button = self.driver.find_element(By.CSS_SELECTOR, "button[data-item-id='oh']")
            hours_button.click()
            sleep_module.sleep(0.3)
            
            # Extraire tous les horaires
            hours_elements = self.driver.find_elements(By.CSS_SELECTOR, "table tr")
            hours = {}
            
            for element in hours_elements:
                day = element.find_element(By.CSS_SELECTOR, "th").text
                opening_time = element.find_element(By.CSS_SELECTOR, "td").text  # Renommé 'time' en 'opening_time'
                hours[day] = opening_time
                
            return hours
        except:
            return None

    def close(self):
        """Fermer le navigateur"""
        self.driver.quit()

    def is_element_valid(self, element):
        """Vérifier si un élément est toujours valide"""
        try:
            element.is_enabled()  # Simple vérification
            return True
        except:
            return False

    def is_element_visible(self, element):
        """Vérifier si un élément est visible"""
        try:
            return element.is_displayed() and element.is_enabled()
        except:
            return False

    def scroll_to_element(self, element):
        """Scroll jusqu'à un élément"""
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                element
            )
        except:
            pass

def get_json_filename(csv_file):
    """Générer le nom du fichier JSON basé sur le nom du fichier CSV"""
    base_name = os.path.basename(csv_file)
    name_without_ext = os.path.splitext(base_name)[0]
    return f"resultats/{name_without_ext}.json"

def save_to_json_safe(result, csv_file, lock):
    """Version thread-safe de la sauvegarde JSON"""
    if not result:
        return
        
    json_file = get_json_filename(csv_file)
    
    with lock:  # Acquérir le verrou
        try:
            # Charger les données existantes
            existing_data = []
            if os.path.exists(json_file):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
            
            # Ajouter les nouvelles données
            if isinstance(existing_data, list):
                existing_data.extend([result])
            else:
                existing_data = [result]
            
            # Sauvegarder toutes les données
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"Erreur lors de la sauvegarde JSON : {str(e)}")

def get_url_hash(url):
    """Générer un hash unique pour une URL"""
    return hashlib.md5(url.encode()).hexdigest()

def load_cache():
    """Charger le cache des URLs déjà traitées"""
    try:
        with open('scraping_cache.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {'processed_urls': {}}

def update_cache(url, status='success', cache=None):
    """Mettre à jour le cache avec une nouvelle URL traitée"""
    if cache is None:
        cache = load_cache()
    
    url_hash = get_url_hash(url)
    cache['processed_urls'][url_hash] = {
        'url': url,
        'status': status,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open('scraping_cache.json', 'w') as f:
        json.dump(cache, f, indent=2)

def get_urls_from_csvs():
    """Récupérer toutes les URLs depuis les fichiers CSV du dossier csvLobstr"""
    all_urls = []
    cache = load_cache()
    
    # Parcourir tous les fichiers CSV dans le dossier
    for csv_file in glob.glob('csvLobstr/*.csv'):
        try:
            df = pd.read_csv(csv_file)
            if 'url' not in df.columns:
                print(f"Colonne 'url' manquante dans {csv_file}")
                continue
                
            # Extraire le métier du nom du fichier
            metier = os.path.basename(csv_file).split()[0].lower()
            
            # Initialiser le fichier JSON correspondant
            json_file = get_json_filename(csv_file)
            if not os.path.exists(json_file):
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump([], f)
            
            # Filtrer les URLs non traitées
            for url in df['url'].tolist():
                url_hash = get_url_hash(url)
                if url_hash not in cache['processed_urls']:
                    all_urls.append((url, metier, csv_file))  # Ajouter le csv_file
                    
        except Exception as e:
            print(f"Erreur lors de la lecture de {csv_file}: {str(e)}")
    
    return all_urls

def scrape_url(args):
    """Fonction qui sera exécutée par chaque worker"""
    url, metier, csv_file, lock = args  # Ajouter csv_file aux arguments
    scraper = GoogleMapsScraper()
    try:
        results = scraper.scrape_listing(url, metier, csv_file, lock)  # Passer csv_file
        update_cache(url, 'success')
        return results
    except Exception as e:
        print(f"Erreur lors du scraping de {url}: {str(e)}")
        update_cache(url, 'error')
        return None
    finally:
        scraper.close()

def main():
    # Créer le dossier de résultats s'il n'existe pas
    os.makedirs('resultats', exist_ok=True)
    
    # Récupérer toutes les URLs à traiter
    urls_to_process = get_urls_from_csvs()
    
    if not urls_to_process:
        print("Aucune nouvelle URL à traiter")
        return
        
    print(f"Nombre d'URLs à traiter : {len(urls_to_process)}")

    # Créer un verrou partagé
    with Manager() as manager:
        lock = manager.Lock()
        
        # Nombre de workers
        n_workers = min(10, len(urls_to_process))
        print(f"Démarrage du scraping avec {n_workers} workers...")

        # Créer un pool de workers
        with Pool(processes=n_workers) as pool:
            # Préparer les arguments pour chaque worker
            worker_args = [(url, metier, csv_file, lock) for url, metier, csv_file in urls_to_process]
            
            # Exécuter le scraping en parallèle
            results = pool.map(scrape_url, worker_args)

    print("Scraping terminé!")

if __name__ == "__main__":
    main()