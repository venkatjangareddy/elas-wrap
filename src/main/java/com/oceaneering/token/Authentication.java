/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.token;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.Gson;
import static com.oceaneering.common.Utility.deleteExpiredTokens;
import static com.oceaneering.common.Utility.isTokenExists;
import com.oceaneering.exceptions.TokenAuthenticationException;
import com.oceaneering.exceptions.UnAuthorizedAccessException;
import com.oceaneering.properties.NetworkProps;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.search.EsRequest;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.json.JsonObject;
import java.util.Map;
import java.util.TimeZone;
import javax.json.JsonArray;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.Node;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import org.w3c.dom.NodeList;

/**
 *
 * @author SKashyap
 *
 */
public class Authentication {

    private String username;
    private String method;
    private String serverURI;
    private String mdsURL;
    private String serverEnv;
    private JsonObject requestNode;
    private WrapperProperties properties;
    private List<String> roles;
    private final String appName;
    private Map<String, String> userInfo = null;

    public Authentication(JsonObject requestNode, WrapperProperties properties) throws UnAuthorizedAccessException, IOException {
        NetworkProps net = NetworkProps.Instance();
        // check for required keys in requestNode else throw bad request exception
        this.properties = properties;
        this.requestNode = requestNode;
        this.serverURI = net.serverURI;
        this.mdsURL = net.mdsUrl;
        this.serverEnv = net.serverEnv;
        this.appName = this.requestNode.getString("app_name");
        this.username = this.requestNode.getString("username");
        this.roles = getUserRoles();
    }

    public String token() throws UnsupportedEncodingException, IOException {
        String token = "";
        String tokenAndUserInfo = null;
        Date date = new Date(System.currentTimeMillis() + properties.getSessionTimeOut() * 1000);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z");
        formatter.setTimeZone(TimeZone.getDefault().getTimeZone("UTC"));
        String signKey = properties.getSignedKey();
        token = Jwts.builder()
                .setIssuer(appName)
                .setExpiration(date)
                .claim("username", username)
                .claim("app_name", appName)
                .claim("serverName", serverEnv)
                .claim("roles", roles.toString())
                .signWith(SignatureAlgorithm.HS256, signKey.getBytes("UTF-8"))
                .compact();
        userInfo.put("jwtToken", token);
        Gson gson = new Gson();
        tokenAndUserInfo = gson.toJson(userInfo);
        WrapperProperties jwtProps = WrapperProperties.Instance("JWT Expiry");
        Map<String, String> tokenToBeSaved = userInfo;
        tokenToBeSaved.put("expiry", formatter.format(date));
        String tokenData = gson.toJson(tokenToBeSaved);
        JsonNode esresponse = (new EsRequest(tokenData, jwtProps))
                .setType(jwtProps.getESType().asText())
                .setEndPoint(token)
                .debug()
                .post();
        return token;
    }

    private List getUserRoles() throws UnAuthorizedAccessException, IOException {
        List<String> userRoles = new ArrayList();
        Map<String, String> userInfo = getUserDetails(username);
        if (!properties.isExternalApp()) {
            if (this.requestNode.containsKey("roles")) {
                JsonArray rolesList = this.requestNode.getJsonArray("roles");
                int size = rolesList.size();
                for (int i = 0; i < size; i++) {
                    userRoles.add(rolesList.get(i).toString());
                }
            } else {
                throw new UnAuthorizedAccessException(106);
            }
        } else {
            if (userRoles.isEmpty() || userRoles.size() <= 0) {
                if (!this.requestNode.containsKey("roles")) {
                    List<String> userRolesThroughMDS = getUserRole(this.requestNode.getString("app_name"), username);
                    if (userRolesThroughMDS != null && userRolesThroughMDS.size() > 0) {
//                        userRoles.add(userRolesThroughMDS.toString());
                        for (int i = 0; i < userRolesThroughMDS.size(); i++) {
                            userRoles.add('"' + userRolesThroughMDS.get(i) + '"');
                        }

                    } else {
                        throw new UnAuthorizedAccessException(106);
                    }
                } else {
                    throw new UnAuthorizedAccessException(118);
                }
            }
        }
        return userRoles;
    }

    private Map<String, String> getUserDetails(String uname) {
        try {
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            SOAPMessage soapResponse = soapConnection.call(createUserInfoSOAPRequest(uname), mdsURL);
            soapResponse.writeTo(System.out);
            userInfo = new HashMap();
            userInfo = parseSoapResponse(soapResponse);
        } catch (Exception ex) {
        }
        return userInfo;
    }

    private Map<String, String> parseSoapResponse(SOAPMessage soapResponse) throws SOAPException {
        String authorized = "false";
        Map<String, String> userInfo = null;
        if (soapResponse.getSOAPBody().getFirstChild().getFirstChild() == null) {
            userInfo = new HashMap();
            userInfo.put("authorize", authorized);
            return userInfo;
        }
        Node sb = (Node) soapResponse.getSOAPBody().getChildElements().next();
        Node user = (Node) sb.getFirstChild().getFirstChild().getNextSibling().getFirstChild().getFirstChild();
        if (user != null) {
            authorized = "true";
            userInfo = new HashMap();
            userInfo.put("authorize", authorized);
            String login = user.getFirstChild().getTextContent();
            userInfo.put("username", login);
            String empId = user.getFirstChild().getNextSibling().getFirstChild().getTextContent();
            userInfo.put("empId", empId);
        }
        return userInfo;
    }

    private SOAPMessage createUserInfoSOAPRequest(String uname) throws SOAPException, IOException {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration("example", serverURI);
        SOAPBody soapBody = envelope.getBody();
        SOAPElement soapBodyElem1 = soapBody
                .addBodyElement(envelope.createName("GetUserInfo", "", serverURI));
        soapBodyElem1.addChildElement("userID").addTextNode(uname);

        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", serverURI + "GetUserInfo");
        soapMessage.writeTo(System.out);
        return soapMessage;
    }

    private SOAPMessage createAppRoleSOAPRequest(String appName, String username) throws SOAPException, IOException {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration("example", serverURI);
        SOAPBody soapBody = envelope.getBody();
        SOAPElement soapBodyElem1 = soapBody
                .addBodyElement(envelope.createName("GetAppRoles", "", serverURI));
        soapBodyElem1.addChildElement("app").addTextNode(appName);
        soapBodyElem1.addChildElement("login").addTextNode(username);
        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", serverURI + "GetAppRoles");
        return soapMessage;
    }

    private SOAPMessage createSOAPRequest(Map<String, String> info) throws SOAPException, IOException {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration("example", serverURI);
        SOAPBody soapBody = envelope.getBody();
        SOAPElement soapBodyElem1 = soapBody
                .addBodyElement(envelope.createName("GetAppRoles", "", serverURI));
        Iterator<Map.Entry<String, String>> itr = info.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            soapBodyElem1.addChildElement(entry.getKey()).addTextNode(entry.getValue());
        }

        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", serverURI + "GetAppRoles");
        return soapMessage;
    }

    private List<String> getUserRole(String appName, String username) {
        List<String> assignedRoles = null;
        try {
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            SOAPMessage soapResponse = soapConnection.call(createAppRoleSOAPRequest(appName, username), mdsURL);
            assignedRoles = parseAppRoleSoapResponse(soapResponse);
        } catch (Exception e) {
            assignedRoles = new ArrayList();
            return assignedRoles;
        }
        return assignedRoles;
    }

    private List<String> parseAppRoleSoapResponse(SOAPMessage soapResponse) throws SOAPException {
        SOAPBody sb = soapResponse.getSOAPBody();
        NodeList nodeList = sb.getFirstChild().getFirstChild().getFirstChild().getChildNodes();
        int length = nodeList.getLength();
        List<String> roles = new ArrayList();
        int i = 0;
        while (i < length) {
            Node node = (Node) nodeList.item(i);
            String roleName = node.getAttributes().getNamedItem("name").getNodeValue().toString();
            roles.add(roleName);
            i++;
        }
        return roles;
    }

    public static Map<String, String> validateToken(String token, String key) throws TokenAuthenticationException, IOException {
        String validated = "false";
        Map<String, String> body = null;
        if (token != null && key != null) {
            try {
                if (isTokenExists(token)) {
                    Jws<Claims> jwtClaims = Jwts.parser().setSigningKey(key.getBytes("UTF-8")).parseClaimsJws(token);
                    body = new HashMap(jwtClaims.getBody());
                } else {
                    throw new TokenAuthenticationException(114);
//                    body = new HashMap();
//                    validated = "false";
//                    body.put("validated", validated);

                }
            } catch (ExpiredJwtException e) {
                deleteExpiredTokens(token);
                throw new TokenAuthenticationException(114);
            } catch (SignatureException | MalformedJwtException e) {
                throw new TokenAuthenticationException(115);
            } catch (UnsupportedJwtException | IllegalArgumentException | UnsupportedEncodingException e) {
                throw new TokenAuthenticationException(116);
            }
        }
        return body;
    }
}
