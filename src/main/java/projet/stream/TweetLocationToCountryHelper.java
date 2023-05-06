package projet.stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TweetLocationToCountryHelper {
    private String database_file;
    public TweetLocationToCountryHelper(String file){
        this.database_file = file;
    }
    public String getCountry(String location_string) throws IOException {
        Reader reader = new FileReader(database_file);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader());
        String standarizedLocationName = location_string.toLowerCase();
        List<String> possibleMatches = new ArrayList<>();
        for (CSVRecord csvRecord : csvParser) {
            String country_name = csvRecord.get("country_name").toLowerCase();
            String country_code = csvRecord.get("country_code").toLowerCase();
            String city_name = csvRecord.get("name").toLowerCase();
            String state_name = csvRecord.get("state_name").toLowerCase();
            String state_code = csvRecord.get("state_code").toLowerCase();
            if (standarizedLocationName.equals(country_name) || standarizedLocationName.equals(country_code)||standarizedLocationName.equals(state_name) || standarizedLocationName.equals(state_code)||standarizedLocationName.equals(city_name)) {
                possibleMatches.add(country_name.toUpperCase());
            } else{
                String[] words = standarizedLocationName.split(",| ");
                if(words.length > 4){
                    return "OTHER";
                }
                if (words[words.length - 1].equals(state_code) || words[words.length - 1].equals(country_code)) {
                    possibleMatches.add(country_name.toUpperCase());
                } else {
                    for (int i = 0; i < words.length - 1; i++) {
                        if (words[i].equals(country_name) || words[i].equals(country_code) || words[i].equals(state_name)
                                || words[i].equals(state_code) || words[i].equals(city_name)) {
                            possibleMatches.add(country_name.toUpperCase());
                        }
                    }
                }
            }
        }
        if (possibleMatches.isEmpty()) {
            return "OTHER";
        } else if (possibleMatches.size() == 1) {
            return possibleMatches.get(0);
        } else {
            // Determine the most likely country based on the number of matches
            Map<String, Integer> countryCounts = new HashMap<>();
            for (String country : possibleMatches) {
                countryCounts.put(country, countryCounts.getOrDefault(country, 0) + 1);
            }
            String mostLikelyCountry = null;
            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : countryCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    mostLikelyCountry = entry.getKey();
                }
            }
            return mostLikelyCountry;
        }
    }
}
