# ChantierSignal — prototype

**Les chantiers de demain, détectés aujourd'hui.** Transforme les autorisations
d'urbanisme (open data SITADEL) en opportunités commerciales géolocalisées pour
les artisans du bâtiment.

## Contenu

| Fichier | Rôle |
|---|---|
| `index.html` | L'application démo : carte interactive, filtres métier, fiches leads. **Double-clique dessus, ça marche tout de suite** (données de démonstration incluses). |
| `pipeline_sitadel.py` | Le pipeline de données : télécharge SITADEL, filtre un département, classifie par métier, géocode, produit `leads.json` + `leads.js`. |
| `leads.json` / `leads.js` | Les données affichées par l'app (actuellement : démo réaliste, dept 44). |

## Passer aux données réelles

```bash
pip install requests pandas
python3 pipeline_sitadel.py --dept 44 --mois 6
# puis rouvrir index.html
```

Le script découvre automatiquement le dernier millésime publié sur la
plateforme DIDO du ministère (jeu "Liste des permis de construire et autres
autorisations d'urbanisme"), tolère les variantes de colonnes, et géocode via
l'API Adresse (gratuite, sans clé). Un workflow GitHub Actions
(`.github/workflows/update-leads.yml`) relance tout ça le 5 de chaque mois.

## Points à connaître

- **Latence** : SITADEL publie avec 1 à 3 mois de décalage. Acceptable : un
  permis autorisé met des mois à devenir un chantier. Le concurrent principal
  (PermisLead) vit avec la même contrainte.
- **RGPD** : les noms des demandeurs particuliers ne sont pas diffusés. Le
  lead exploitable = l'adresse du projet → démarchage par courrier/boîtage,
  ou repérage terrain. Les demandeurs personnes morales (promoteurs, SCI)
  sont identifiés avec leur SIREN : eux sont prospectables directement.
- **Classification métier** : par mots-clés sur la nature du projet
  (dictionnaire `METIERS` dans le script). À enrichir avec les retours de
  vrais artisans.

## Validation avant d'aller plus loin (2 semaines)

1. Générer les données réelles de TON département.
2. Montrer l'app à 10 artisans (piscinistes et couvreurs d'abord : leads les
   plus qualifiés). Question unique : « tu paierais 59 €/mois pour ça ? »
3. 3 oui fermes → construire la v1 SaaS (comptes, alertes email hebdo,
   paiement Stripe). 0 oui → pivoter la cible (promoteurs ? négoces matériaux ?)
   ou enterrer, coût total : deux semaines.

## Roadmap v1 (si validé)

- Comptes + abonnement Stripe (59 €/mois, 14 j d'essai)
- Alerte email hebdo par métier + rayon (le vrai produit : l'artisan ne se
  connecte pas, il reçoit)
- Ingestion mensuelle automatique (cron)
- Enrichissement : DVF (prix du foncier), permis modificatifs, mise en
  relation courrier ("envoyez une carte postale à ce chantier en 1 clic")
