package class2;

import java.util.Set;

public class UserVisitSite {

    public String user;
    public Set<String> urlSet;

    public UserVisitSite(Set<String> urlSet, String user) {
        this.urlSet = urlSet;
        this.user = user;
    }

    public UserVisitSite() {
    }

    @Override
    public String toString() {
        return "UserVisitSite{" +
                "user='" + user + '\'' +
                ", urlSet=" + urlSet +
                '}';
    }
}
