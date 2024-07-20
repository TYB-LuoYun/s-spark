package top.anets;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CloudFriendAppllication {
        public static void main(String[] args) { SpringApplication.run(CloudFriendAppllication.class); }
}
