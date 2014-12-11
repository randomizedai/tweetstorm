#!/bin/bash
text="species extinction,Climate change,cause"
#sea level rise,Climate change,cause;Poverty,Climate change,cause;weather extremes,Climate change,cause;Climate change,GHG emissions,cause;GHG emissions,Fossil Fuels,cause;GHG emissions,Deforestation,cause;GHG emissions,Transportation,cause;GHG emissions,Clean Development Mechanism,decrease;GHG emissions,compliance enforcement,decrease"
# $1 - number of pages for news/sci to read
IFS=';' read -a array <<< "$text"
for element in "${array[@]}"
do
    IFS=',' read -a subarray <<< "$element"
    echo "${subarray[0]}"
    echo "${subarray[1]}"
    #python get_verbal_phrase.py -y news -a "${subarray[0]}" -b "${subarray[1]}" -n $1 -t 2
    #python get_verbal_phrase.py -y twitter -a "${subarray[0]}" -b "${subarray[1]}" < fts.json
    #python get_verbal_phrase.py -y twitter -a "${subarray[0]}" -b "${subarray[1]}" -s < fts.json
    python get_verbal_phrase.py -y twitter -a "${subarray[0]}" -b "${subarray[1]}" -o < fts.json
done

