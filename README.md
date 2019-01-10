# Τα αρχεία αυτά τα χρησιμοποιώ για μετατροπή των CLASS/NOAA hdf σε geotiff. #

Πιο συγκεκριμένα:

* batchhdf2geotiff.py: για την μετατροπή των αρχείων SDR (GDNBO-SVDNB_npp_*.h5) και των datasets
  Radiance και LunarAzimuthAngle
  
* batchEDR2geotiff.py: για την μετατροπή των αρχείων EDR (GMODO-VICMO_npp_*.h5),
  του dataset QF1_VIIRSCMEDR. Με αυτό το αρχείο κάνω απλη μετατροπή δεν βγάζω
  CloudMask. Το CloudMask βγαίνε στην συνέχεια στην R.
  
* batchDNB2geotiff.py: Αυτό το αρχείο δεν το χρησιμοποιώ πουθενά. Το κρατάω
  γιατί εχει δυνατότητες histogram stretching και για πιθανές ανάγκες οπτικοποίησης.


