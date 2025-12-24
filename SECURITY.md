# Security Best Practices

## Secret Management

This application supports multiple methods for securely managing sensitive configuration data like MQTT passwords.

### Method 1: Environment Variables (Recommended for Production)

Set the MQTT password as an environment variable:

```bash
export MQTT_PASSWORD="your-secure-password"
dotnet run
```

Or in Docker:

```dockerfile
ENV MQTT_PASSWORD="your-secure-password"
```

Or in Kubernetes:

```yaml
env:
  - name: MQTT_PASSWORD
    valueFrom:
      secretKeyRef:
        name: mqtt-secrets
        key: password
```

### Method 2: User Secrets (Recommended for Development)

For local development, use .NET User Secrets to keep credentials out of source control:

```bash
cd service
dotnet user-secrets init
dotnet user-secrets set "MQTT_PASSWORD" "your-dev-password"
```

### Method 3: Configuration File (Not Recommended)

You can store the password in `appsettings.json`, but this is **not recommended** for production:

```json
{
  "Mqtt": {
    "Password": "your-password"
  }
}
```

**Never commit passwords to source control!** The `.gitignore` file should exclude `appsettings.*.json` files.

## Priority Order

The application loads configuration in the following order (later sources override earlier ones):

1. `appsettings.json`
2. `appsettings.{Environment}.json`
3. User Secrets (Development only)
4. Environment Variables
5. `MQTT_PASSWORD` environment variable (highest priority)

## Additional Security Recommendations

1. **Rotate credentials regularly**: Change MQTT passwords periodically
2. **Use TLS/SSL**: Configure the MQTT broker to use encrypted connections
3. **Principle of least privilege**: Use MQTT credentials with minimal required permissions
4. **Audit access**: Enable logging on the MQTT broker to track connection attempts
5. **Secure the database**: Use SQL Server authentication with strong passwords and restrict network access
