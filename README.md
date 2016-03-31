### Analysis of Healthcare Cost and Utilization in the First Year of the Medicare Shared Savings Program Using Big Data from the CMS Enclave
##### Fabrício S. P. Kury, MD\*1, Seo H. Baik, PhD\*1, Clement J. McDonald, MD\*1
##### \*1 Lister Hill National Center for Biomedical Communications, National Library of Medicine, U.S. National Institutes of Health, Bethesda, Maryland, USA
###### Codename: cisa
  
This is the repository with the accompanying full source code and full tables of results of the AMIA 2016 Annual Symposium paper. All materials contained in this GitHub repository are under an Attribution-NonCommercial-ShareAlike 4.0 International license. Please see details at http://creativecommons.org/licenses/by-nc-sa/4.0/.  
  
**_We, authors, have the following corrections to the submitted paper. All contents in this GitHub repository are already corrected. Although these corrections modify the resulting numbers by magnitudes varying from marginal to partial, the corrected results do not alter the conclusions of the paper._**  
* **_Where it is written that the "Percent difference" column is [non-ACO value] minus [ACO value] divided by [unsigned non-ACO value], the correct is "the 'Percent difference' column is ([non-ACO value] minus [ACO value]) divided by [non-ACO value]."_**  
* **_An error in our code for processing the MedPAR file inflated all its numbers by 12-fold in both non-ACO and ACO cohorts. Because the error applied equally to both cohorts, it had no impact on the Percent difference numbers._**  
* **_Upon observation of the histograms of total cost per claim, we identified a minority of outlier claims whose cost was too high to be likely correct (e.g. over ten million dollars in a single claim). We decided to exclude outlier claims from the analysis in order to minimize noise. An outlier claim was defined as a claim whose cost was at the top 0.01-th percentile (yes, top 0.01%, not 1%) of its type. This alteration did impact the magnitude of some numbers, although hardly ever changing a difference from negative to positive or positive to negative._**
  
Please feel free to contact us about this project! E-mail Fabrício directly: *fabricio.kury at nih.gov*.  
Reading and reusing source code can become so much easier after a quick voice talk with the original authors of the code -- we will be glad to help.  
  
Best regards,  
Fabrício Kury  
Seo Hyon Baik  
Clement McDonald
