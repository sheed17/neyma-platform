"use client";

import * as React from "react";
import { useCallback, useState } from "react";
import {
  AlertTriangle,
  Eye,
  EyeOff,
  KeyRound,
  Loader2,
  Lock,
  Mail,
  Phone,
  Shield,
  User,
} from "lucide-react";

import { cn } from "@/lib/utils";

type AuthMode = "login" | "signup" | "reset";
type RegistrationStep = "details" | "complete";

type AuthSuccessData = {
  email: string;
  name?: string;
};

interface AuthFormProps {
  onSuccess?: (userData: AuthSuccessData) => void;
  onClose?: () => void;
  initialMode?: AuthMode;
  className?: string;
  onLogin?: (payload: { email: string; password: string; rememberMe: boolean }) => Promise<void>;
  onSignup?: (payload: { name: string; email: string; password: string; phone?: string }) => Promise<void>;
  onResetPassword?: (payload: { email: string }) => Promise<void>;
  onUseTestAccount?: () => Promise<void>;
  testAccountLabel?: string;
  testAccountHint?: string;
}

interface FormData {
  name: string;
  email: string;
  password: string;
  confirmPassword: string;
  phone: string;
  agreeToTerms: boolean;
  rememberMe: boolean;
  verificationCode: string;
}

interface FormErrors {
  name?: string;
  email?: string;
  password?: string;
  confirmPassword?: string;
  phone?: string;
  agreeToTerms?: string;
  general?: string;
  verificationCode?: string;
}

interface PasswordStrength {
  score: number;
  feedback: string[];
  requirements: {
    length: boolean;
    uppercase: boolean;
    lowercase: boolean;
    number: boolean;
    special: boolean;
  };
}

const calculatePasswordStrength = (password: string): PasswordStrength => {
  const requirements = {
    length: password.length >= 8,
    uppercase: /[A-Z]/.test(password),
    lowercase: /[a-z]/.test(password),
    number: /\d/.test(password),
    special: /[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?]/.test(password),
  };

  const score = Object.values(requirements).filter(Boolean).length;
  const feedback: string[] = [];

  if (!requirements.length) feedback.push("At least 8 characters");
  if (!requirements.uppercase) feedback.push("One uppercase letter");
  if (!requirements.lowercase) feedback.push("One lowercase letter");
  if (!requirements.number) feedback.push("One number");
  if (!requirements.special) feedback.push("One special character");

  return { score, feedback, requirements };
};

const PasswordStrengthIndicator: React.FC<{ password: string }> = ({ password }) => {
  const strength = calculatePasswordStrength(password);

  const getStrengthColor = (score: number) => {
    if (score <= 1) return "text-destructive";
    if (score <= 2) return "text-orange-500";
    if (score <= 3) return "text-yellow-500";
    if (score <= 4) return "text-zinc-300";
    return "text-primary";
  };

  const getStrengthText = (score: number) => {
    if (score <= 1) return "Very Weak";
    if (score <= 2) return "Weak";
    if (score <= 3) return "Fair";
    if (score <= 4) return "Good";
    return "Strong";
  };

  if (!password) return null;

  return (
    <div className="mt-2 space-y-2">
      <div className="flex items-center gap-2">
        <div className="h-2 flex-1 overflow-hidden rounded-full bg-muted">
          <div
            className={cn("h-full rounded-full bg-current", getStrengthColor(strength.score))}
            style={{ width: `${(strength.score / 5) * 100}%` }}
          />
        </div>
        <span className="min-w-[60px] text-xs text-muted-foreground">
          {getStrengthText(strength.score)}
        </span>
      </div>
      {strength.feedback.length > 0 ? (
        <div className="grid grid-cols-2 gap-1">
          {strength.feedback.map((item) => (
            <div
              key={item}
              className="flex items-center gap-1 text-xs text-amber-500 dark:text-amber-400"
            >
              <AlertTriangle className="h-3 w-3" />
              <span>{item}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
};

export function AuthForm({
  onSuccess,
  onClose,
  initialMode = "login",
  className,
  onLogin,
  onSignup,
  onResetPassword,
  onUseTestAccount,
  testAccountLabel = "Use test account",
  testAccountHint = "Local shortcut for workspace access",
}: AuthFormProps) {
  const [authMode, setAuthMode] = useState<AuthMode>(initialMode);
  const [registrationStep, setRegistrationStep] = useState<RegistrationStep>("details");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState("");
  const [formData, setFormData] = useState<FormData>({
    name: "",
    email: "",
    password: "",
    confirmPassword: "",
    phone: "",
    agreeToTerms: false,
    rememberMe: false,
    verificationCode: "",
  });
  const [errors, setErrors] = useState<FormErrors>({});
  const [fieldTouched, setFieldTouched] = useState<Record<string, boolean>>({});

  React.useEffect(() => {
    const savedEmail = localStorage.getItem("userEmail");
    const rememberMe = localStorage.getItem("rememberMe") === "true";
    if (savedEmail && authMode === "login") {
      setFormData((prev) => ({ ...prev, email: savedEmail, rememberMe }));
    }
  }, [authMode]);

  const resetMessages = useCallback(() => {
    setErrors({});
    setSuccessMessage("");
  }, []);

  const switchMode = useCallback((mode: AuthMode) => {
    setAuthMode(mode);
    if (mode !== "signup") {
      setRegistrationStep("details");
    }
    resetMessages();
  }, [resetMessages]);

  const validateField = useCallback(
    (field: keyof FormData, value: string | boolean) => {
      let error = "";

      switch (field) {
        case "name":
          if (typeof value === "string" && authMode === "signup" && !value.trim()) {
            error = "Name is required";
          }
          break;
        case "email":
          if (!value || (typeof value === "string" && !value.trim())) {
            error = "Email is required";
          } else if (
            typeof value === "string" &&
            !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
          ) {
            error = "Please enter a valid email address";
          }
          break;
        case "password":
          if (!value) {
            error = "Password is required";
          } else if (typeof value === "string") {
            if (value.length < 8) {
              error = "Password must be at least 8 characters";
            } else if (authMode === "signup") {
              const strength = calculatePasswordStrength(value);
              if (strength.score < 3) {
                error = "Password is too weak";
              }
            }
          }
          break;
        case "confirmPassword":
          if (authMode === "signup" && value !== formData.password) {
            error = "Passwords do not match";
          }
          break;
        case "phone":
          if (
            typeof value === "string" &&
            value &&
            !/^\+?[\d\s\-()]+$/.test(value)
          ) {
            error = "Please enter a valid phone number";
          }
          break;
        case "agreeToTerms":
          if (authMode === "signup" && !value) {
            error = "You must agree to the terms and conditions";
          }
          break;
        default:
          break;
      }

      return error;
    },
    [authMode, formData.password]
  );

  const handleInputChange = useCallback(
    (field: keyof FormData, value: string | boolean) => {
      setFormData((prev) => ({ ...prev, [field]: value }));
      if (fieldTouched[field]) {
        const error = validateField(field, value);
        setErrors((prev) => ({ ...prev, [field]: error || undefined }));
      }
    },
    [fieldTouched, validateField]
  );

  const handleFieldBlur = useCallback(
    (field: keyof FormData) => {
      setFieldTouched((prev) => ({ ...prev, [field]: true }));
      const value = formData[field];
      const error = validateField(field, value);
      setErrors((prev) => ({ ...prev, [field]: error || undefined }));
    },
    [formData, validateField]
  );

  const validateForm = useCallback(() => {
    const newErrors: FormErrors = {};
    const fieldsToValidate: (keyof FormData)[] = ["email"];

    if (authMode !== "reset") {
      fieldsToValidate.push("password");
    }
    if (authMode === "signup") {
      fieldsToValidate.push("name", "confirmPassword", "agreeToTerms");
    }
    fieldsToValidate.forEach((field) => {
      const error = validateField(field, formData[field]);
      if (error) newErrors[field] = error;
    });

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }, [authMode, formData, validateField]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    resetMessages();
    if (!validateForm()) return;

    setIsLoading(true);

    try {
      if (authMode === "login") {
        if (onLogin) {
          await onLogin({
            email: formData.email.trim(),
            password: formData.password,
            rememberMe: formData.rememberMe,
          });
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }

        if (formData.rememberMe) {
          localStorage.setItem("userEmail", formData.email);
          localStorage.setItem("rememberMe", "true");
        } else {
          localStorage.removeItem("userEmail");
          localStorage.removeItem("rememberMe");
        }

        setSuccessMessage("Login successful");
        onSuccess?.({ email: formData.email.trim() });
      } else if (authMode === "signup") {
        if (onSignup) {
          await onSignup({
            name: formData.name.trim(),
            email: formData.email.trim(),
            password: formData.password,
            phone: formData.phone.trim() || undefined,
          });
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }

        setRegistrationStep("complete");
        setSuccessMessage("Account created successfully");
        onSuccess?.({ email: formData.email.trim(), name: formData.name.trim() });
      } else if (authMode === "reset") {
        if (onResetPassword) {
          await onResetPassword({ email: formData.email.trim() });
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        setSuccessMessage("Password reset email sent");
        setTimeout(() => switchMode("login"), 1200);
      }
    } catch (error) {
      const message =
        error instanceof Error && error.message
          ? error.message
          : "Authentication failed. Please try again.";
      setErrors({ general: message });
    } finally {
      setIsLoading(false);
    }
  };

  const handleUseTestAccount = async () => {
    if (!onUseTestAccount) return;
    resetMessages();
    setIsLoading(true);
    try {
      await onUseTestAccount();
      setSuccessMessage("Test account ready");
      onSuccess?.({ email: "test@neyma.local", name: "Neyma Test" });
    } catch (error) {
      const message =
        error instanceof Error && error.message
          ? error.message
          : "Couldn't open the test account right now.";
      setErrors({ general: message });
    } finally {
      setIsLoading(false);
    }
  };

  const renderFieldError = (id: string, message?: string, centered?: boolean) =>
    message ? (
      <p
        id={id}
        className={cn(
          "mt-1 flex items-center gap-1 text-xs text-destructive",
          centered && "justify-center"
        )}
      >
        <AlertTriangle className="h-3 w-3" />
        {message}
      </p>
    ) : null;

  const renderAuthContent = () => {
    if (authMode === "reset") {
      return (
        <div className="space-y-4">
          <div className="mb-6 text-center">
            <KeyRound className="mx-auto mb-3 h-12 w-12 text-primary" />
            <h3 className="mb-2 text-xl font-semibold">Password Recovery</h3>
            <p className="text-sm text-muted-foreground">
              Enter your email address and we&apos;ll send you a link to reset your password.
            </p>
          </div>

          <div className="relative">
            <Mail className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <input
              type="email"
              placeholder="Email Address"
              autoComplete="email"
              value={formData.email}
              onChange={(e) => handleInputChange("email", e.target.value)}
              onBlur={() => handleFieldBlur("email")}
              className={cn(
                "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-4 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                errors.email ? "border-destructive" : "border-input"
              )}
              aria-label="Email Address"
              aria-describedby={errors.email ? "email-error" : undefined}
            />
            {renderFieldError("email-error", errors.email)}
          </div>

          <button
            type="submit"
            disabled={isLoading || !formData.email}
            className={cn(
              "w-full rounded-xl bg-primary px-6 py-3 font-medium text-primary-foreground transition-all hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-primary/20 disabled:opacity-50"
            )}
          >
            <span className="flex items-center justify-center gap-2">
              {isLoading ? (
                <Loader2 className="h-5 w-5 animate-spin" />
              ) : (
                <>
                  <KeyRound className="h-5 w-5" />
                  Send Reset Link
                </>
              )}
            </span>
          </button>

          <div className="text-center">
            <button
              type="button"
              onClick={() => switchMode("login")}
              className="text-sm text-primary transition-colors hover:text-primary/80"
            >
              Back to Login
            </button>
          </div>
        </div>
      );
    }

    if (authMode === "signup" && registrationStep === "complete") {
      return (
        <div className="space-y-6 text-center">
          <div className="inline-flex h-16 w-16 items-center justify-center rounded-full bg-green-100 text-green-600 dark:bg-green-900 dark:text-green-400">
            <svg
              className="h-8 w-8"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M5 13l4 4L19 7"
              />
            </svg>
          </div>

          <div>
            <h3 className="mb-2 text-2xl font-bold">Welcome Aboard</h3>
            <p className="text-muted-foreground">Your account is set up. Check your email to confirm it, then log in to open the workspace.</p>
          </div>

          <button
            type="button"
            onClick={onClose}
            className={cn(
              "w-full rounded-xl bg-primary px-6 py-3 font-medium text-primary-foreground hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-primary/20"
            )}
          >
            Go to Login
          </button>
        </div>
      );
    }

    return (
      <div className="space-y-4">
        {authMode === "signup" ? (
          <div>
            <div className="relative">
              <User className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <input
              type="text"
              placeholder="Full Name"
              autoComplete="name"
              value={formData.name}
              onChange={(e) => handleInputChange("name", e.target.value)}
                onBlur={() => handleFieldBlur("name")}
                className={cn(
                  "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-4 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                  errors.name ? "border-destructive" : "border-input"
                )}
                aria-label="Full Name"
                aria-describedby={errors.name ? "name-error" : undefined}
              />
              {renderFieldError("name-error", errors.name)}
            </div>
          </div>
        ) : null}

        <div>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <input
              type="email"
              placeholder="Email Address"
              autoComplete="email"
              value={formData.email}
              onChange={(e) => handleInputChange("email", e.target.value)}
              onBlur={() => handleFieldBlur("email")}
              className={cn(
                "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-4 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                errors.email ? "border-destructive" : "border-input"
              )}
              aria-label="Email Address"
              aria-describedby={errors.email ? "email-error" : undefined}
            />
            {renderFieldError("email-error", errors.email)}
          </div>
        </div>

        <div>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <input
              type={showPassword ? "text" : "password"}
              placeholder="Password"
              autoComplete={authMode === "signup" ? "new-password" : "current-password"}
              value={formData.password}
              onChange={(e) => handleInputChange("password", e.target.value)}
              onBlur={() => handleFieldBlur("password")}
              className={cn(
                "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-12 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                errors.password ? "border-destructive" : "border-input"
              )}
              aria-label="Password"
              aria-describedby={errors.password ? "password-error" : undefined}
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
              aria-label={showPassword ? "Hide password" : "Show password"}
            >
              {showPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
            </button>
            {renderFieldError("password-error", errors.password)}
          </div>
          {authMode === "signup" ? (
            <PasswordStrengthIndicator password={formData.password} />
          ) : null}
        </div>

        {authMode === "signup" ? (
          <div>
            <div className="relative">
              <Shield className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
              <input
                type={showConfirmPassword ? "text" : "password"}
                placeholder="Confirm Password"
                autoComplete="new-password"
                value={formData.confirmPassword}
                onChange={(e) => handleInputChange("confirmPassword", e.target.value)}
                onBlur={() => handleFieldBlur("confirmPassword")}
                className={cn(
                  "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-12 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                  errors.confirmPassword ? "border-destructive" : "border-input"
                )}
                aria-label="Confirm Password"
                aria-describedby={errors.confirmPassword ? "confirm-password-error" : undefined}
              />
              <button
                type="button"
                onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
                aria-label={showConfirmPassword ? "Hide confirm password" : "Show confirm password"}
              >
                {showConfirmPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
              </button>
              {renderFieldError("confirm-password-error", errors.confirmPassword)}
            </div>
          </div>
        ) : null}

        {authMode === "signup" ? (
          <div>
            <div className="relative">
              <Phone className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
              <input
                type="tel"
                placeholder="Phone Number (Optional)"
                value={formData.phone}
                onChange={(e) => handleInputChange("phone", e.target.value)}
                onBlur={() => handleFieldBlur("phone")}
                className={cn(
                  "w-full rounded-xl border bg-muted/50 py-3 pl-10 pr-4 placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20",
                  errors.phone ? "border-destructive" : "border-input"
                )}
                aria-label="Phone Number"
                aria-describedby={errors.phone ? "phone-error" : undefined}
              />
              {renderFieldError("phone-error", errors.phone)}
            </div>
          </div>
        ) : null}

        <div className="flex items-center justify-between gap-4">
          {authMode === "login" ? (
            <>
              <label className="flex cursor-pointer items-center gap-2">
                <input
                  type="checkbox"
                  checked={formData.rememberMe}
                  onChange={(e) => handleInputChange("rememberMe", e.target.checked)}
                  aria-label="Remember me"
                  className="h-4 w-4 rounded border-input bg-muted text-primary focus:ring-primary focus:ring-offset-0"
                />
                <span className="text-sm text-muted-foreground">Remember me</span>
              </label>
              <button
                type="button"
                onClick={() => switchMode("reset")}
                className="text-sm text-primary transition-colors hover:text-primary/80"
              >
                Forgot password?
              </button>
            </>
          ) : (
            <label className="flex cursor-pointer items-start gap-2">
              <input
                type="checkbox"
                checked={formData.agreeToTerms}
                onChange={(e) => handleInputChange("agreeToTerms", e.target.checked)}
                className="mt-0.5 h-4 w-4 rounded border-input bg-muted text-primary focus:ring-primary focus:ring-offset-0"
                aria-describedby={errors.agreeToTerms ? "terms-error" : undefined}
              />
              <span className="text-sm text-muted-foreground">
                I agree to the{" "}
                <a href="#" className="text-primary transition-colors hover:underline">
                  Terms of Service
                </a>{" "}
                and{" "}
                <a href="#" className="text-primary transition-colors hover:underline">
                  Privacy Policy
                </a>
              </span>
            </label>
          )}
        </div>

        {renderFieldError("terms-error", errors.agreeToTerms)}

        <button
          type="submit"
          disabled={isLoading}
          className={cn(
            "w-full rounded-xl bg-primary px-6 py-3 font-medium text-primary-foreground transition-all hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-primary/20 disabled:opacity-50"
          )}
        >
          <span className="flex items-center justify-center gap-2">
            {isLoading ? (
              <Loader2 className="h-5 w-5 animate-spin" />
            ) : authMode === "login" ? (
              "Sign In"
            ) : (
              "Create Account"
            )}
          </span>
        </button>

        {onUseTestAccount ? (
          <div className="rounded-xl border border-border bg-muted/40 px-4 py-3">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <p className="text-sm font-medium text-foreground">{testAccountLabel}</p>
                <p className="mt-1 text-xs text-muted-foreground">{testAccountHint}</p>
              </div>
              <button
                type="button"
                onClick={() => void handleUseTestAccount()}
                disabled={isLoading}
                className="inline-flex w-full items-center justify-center rounded-xl border border-input bg-background px-4 py-2.5 text-sm font-medium text-foreground transition hover:bg-muted/60 disabled:opacity-50 sm:w-auto"
              >
                Open test account
              </button>
            </div>
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <div
      className={cn("p-6", className)}
      role="dialog"
      aria-modal="true"
      aria-labelledby="auth-title"
    >
      {successMessage ? (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-green-400/30 bg-green-500/20 p-3">
          <svg
            className="h-4 w-4 text-green-500"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M5 13l4 4L19 7"
            />
          </svg>
          <span className="text-sm text-green-700 dark:text-green-300">{successMessage}</span>
        </div>
      ) : null}

      {errors.general ? (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-destructive/30 bg-destructive/20 p-3">
          <AlertTriangle className="h-4 w-4 text-destructive" />
          <span className="text-sm text-destructive">{errors.general}</span>
        </div>
      ) : null}

      <div className="mb-8 text-center">
        <h2 id="auth-title" className="mb-2 text-2xl font-bold">
          {authMode === "login"
            ? "Welcome Back"
            : authMode === "reset"
              ? "Reset Password"
              : "Create Account"}
        </h2>
        <p className="text-muted-foreground">
          {authMode === "login"
            ? "Sign in to your account"
            : authMode === "reset"
              ? "Recover your account access"
              : "Create a new account"}
        </p>
      </div>

      {authMode !== "reset" ? (
        <div className="mb-6 flex rounded-xl bg-muted p-1">
          <button
            onClick={() => switchMode("login")}
            className={cn(
              "flex-1 rounded-lg px-4 py-2 text-sm font-medium transition-all",
              authMode === "login"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            )}
            type="button"
          >
            Login
          </button>
          <button
            onClick={() => {
              switchMode("signup");
              setRegistrationStep("details");
            }}
            className={cn(
              "flex-1 rounded-lg px-4 py-2 text-sm font-medium transition-all",
              authMode === "signup"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            )}
            type="button"
          >
            Sign Up
          </button>
        </div>
      ) : null}

      <form onSubmit={handleSubmit}>{renderAuthContent()}</form>

      {authMode !== "reset" && registrationStep === "details" ? (
        <div className="mt-6 text-center">
          <p className="text-sm text-muted-foreground">
            {authMode === "login" ? "Don't have an account? " : "Already have an account? "}
            <button
              type="button"
              onClick={() => switchMode(authMode === "login" ? "signup" : "login")}
              className="font-medium text-primary transition-colors hover:text-primary/80"
            >
              {authMode === "login" ? "Sign up" : "Sign in"}
            </button>
          </p>
        </div>
      ) : null}
    </div>
  );
}
