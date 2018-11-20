package loginStream;

import java.util.Date;

public class LoginAttackCount {
    String userName;
    Long counter;
    Long start;
    Long to;

    public LoginAttackCount(String userName, Long counter, Long start, Long to) {
        this.userName = userName;
        this.counter = counter;
        this.start = start;
        this.to = to;
    }



    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getCounter() {
        return counter;
    }

    public void setCounter(Long counter) {
        this.counter = counter;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getTo() {
        return to;
    }

    public void setTo(Long to) {
        this.to = to;
    }
}
