package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;

    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {

        super(mongoClient, databaseName);

        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        log = LoggerFactory.getLogger(this.getClass());

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
        try {
            usersCollection.insertOne(user);
            return true;
        }catch (MongoWriteException e) {
            throw new IncorrectDaoOperation(e.getMessage());
        }
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
//        Session alreadyExist = sessionsCollection.find(Filters.eq("jwt", jwt)).first();
//        if(alreadyExist != null){
//            throw new IncorrectDaoOperation("user already has same jwt");
//        }
        try {
            boolean deletedExistingUserSessions = deleteUserSessions(userId);
            if (deletedExistingUserSessions) {
                Session session = new Session();
                session.setJwt(jwt);
                session.setUserId(userId);
                sessionsCollection.insertOne(session);
                return getUserSession(userId) != null;
            }
        }catch (MongoWriteException e){
            System.out.println(e.getError().getCategory());
        }

        return false;

    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;
        user = usersCollection.find(Filters.eq("email", email)).first();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        return sessionsCollection.find(Filters.eq("user_id", userId)).first();
    }

    public boolean deleteUserSessions(String userId) {

        Bson findUserWithUserID = Filters.eq("user_id", userId);
        DeleteResult deleteResult = sessionsCollection.deleteMany(findUserWithUserID);

        return deleteResult.wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        try {
            deleteUserSessions(email);
            DeleteResult deleteResult = usersCollection.deleteOne(Filters.eq("email", email));
            return deleteResult.wasAcknowledged();
        }catch (MongoWriteException e){
            System.out.println(e.getError().getCategory());
        }

        return false;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        if(userPreferences == null){
            throw new IncorrectDaoOperation("User preferences cannot be null");
        }

        Bson filter = new Document("email", email);
        try {
            UpdateResult updateResult = usersCollection.updateOne(filter, set("preferences", userPreferences));
            return updateResult.wasAcknowledged();
        }catch (MongoWriteException e){
            System.out.println(e.getError().getCategory());
        }

        return false;
    }
}
