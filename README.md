# Mετατροπή των CLASS/NOAA hdf σε geotiff.

Scripts για την μετατροπή των αρχείων hdf σε geotiff:

1.  VIIRS Day Night Band SDR (SVDNB)/VIIRS Day Night Band SDR Ellipsoid
    Geolocation (GDNBO).
2.  VIIRS Cloud Mask EDR (VICMO)/VIIRS Moderate Bands SDR Geolocation (GMODO) 

Η λήψη δεδομένων νυκτερινών εικόνων γίνεται από τον διαδικτυακό
τόπο Comprehensive Large Array-data Stewardship System (CLASS)/NOAA (<https://www.bou.class.noaa.gov>).
Τα δεδομένα διατίθενται σε μορφή HDF-EOS.

Και στις δύο περιπτώσεις (GDNBO,VICMO) τα δεδομένα παραγγέλθηκαν με ενσωματωμένα τα αντίστοιχα geolocation files  (GDNBO,GMODO).

Τα dataset που εξάγονται απο τα αρχεία SDR είναι:

-   Radiance, που περιλαμβάνει την ακτινοβολία σε  Wcm^-2^sr^-1^ .

Τα dataset που χρησιμοποιήθηκαν απο τα αρχεία ΕDR είναι: 

-   Cloud Detection Confidence Pixel mask
-   Snow/Ice Surface mask
-   Fire mask

Κατά την μετατροπή των αρχείων από hdf σε geotiff εφαρμόζονται φίλτρα (masking)
με βάση τα απαραίτητα Quality flags για την εξαγωγή των καταλληλότερων (από ποιοτικής άποψης) δεδομένων. Πιο συγκεκριμένα, για τα δεδομένα εφαρμόστηκαν μια σειρά φίλτρων:

-   Radiance Fill values mask
-   Edge-of-swath pixels (aggregation zones 29-32)
-   Missing Data mask
-   Saturated Pixel mask
-   Out of Range mask
-   Pixel-level quality flag (Invalid Input Data, Bad Pointing, Bad Terrain, Invalid Solar Angles)
-   CloudDetection Confidence Pixel mask (τα δεδομένα που χαρακτηρίζονται
    ως Confidently Clear)
-   Snow/Ice Surface mask
-   Fire mask

Για την ανάγνωση των αρχείων hdf, την μετατροπή τους σε geotiff, την αποκοπή των
δεδομένων στα όρια της περιοχής μελέτης και την προβολή τους στo ΕΓΣΑ'87
χρησιμοποιείται η βιβλιοθήκη της Python, [satpy](https://github.com/pytroll/satpy).
H εφαρμογή των επιμέρους μασκών γίνεται μέσω της python βιβλιοθήκης numpy
(logical OR) ενώ η διόρθωση των bow-tie deletion pixels γίνεται μέσω της
βιβλιοθήκης [pyresample](https://pyresample.readthedocs.io).
Τα εξαγώμενα δεδομένα έχουν χωρική διακριτική ικανότητα (resolution) 750m.


Πιο συγκεκριμένα τα scripts είναι:

* batchhdf2geotiff.py: για την μετατροπή των αρχείων SDR (GDNBO-SVDNB_npp_*.h5) και των datasets
  Radiance και LunarAzimuthAngle
  
* batchEDR2geotiff.py: για την μετατροπή των αρχείων EDR (GMODO-VICMO_npp_*.h5),
  του dataset QF1_VIIRSCMEDR. 



