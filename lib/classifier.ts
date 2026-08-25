export type LifecycleCategory =
  | 'discovery-requirements'
  | 'architecture-design'
  | 'project-management'
  | 'ux-product-design'
  | 'development'
  | 'frontend'
  | 'backend-api'
  | 'database-data'
  | 'ai-ml'
  | 'testing-qa'
  | 'security'
  | 'code-quality-review'
  | 'ci-cd-automation'
  | 'cloud-infrastructure'
  | 'deployment-release'
  | 'observability-operations'
  | 'maintenance-debugging'
  | 'documentation-knowledge'
  | 'collaboration-governance';

export type ClassificationDecision = 'KEEP' | 'REVIEW' | 'OUT_OF_SCOPE';

export type SkillClassification = {
  decision: ClassificationDecision;
  score: number;
  categories: LifecycleCategory[];
  matchedSignals: string[];
  reason: string;
};

type Rule = {
  category: LifecycleCategory;
  weight: number;
  terms: RegExp[];
};

const RULES: Rule[] = [
  { category: 'discovery-requirements', weight: 3, terms: [/requirement/i, /user stor/i, /acceptance criteria/i, /product discovery/i, /research/i, /specification/i, /prd\b/i, /backlog/i] },
  { category: 'architecture-design', weight: 3, terms: [/architecture/i, /system design/i, /design pattern/i, /ddd\b/i, /microservice/i, /monolith/i, /event[- ]driven/i, /scalab/i] },
  { category: 'project-management', weight: 2, terms: [/project management/i, /sprint/i, /scrum/i, /kanban/i, /roadmap/i, /milestone/i, /estimat/i, /planning/i, /ticket/i, /issue/i] },
  { category: 'ux-product-design', weight: 2, terms: [/ux\b/i, /ui\b/i, /product design/i, /wireframe/i, /prototype/i, /accessibility/i, /design system/i, /figma/i] },
  { category: 'development', weight: 3, terms: [/software/i, /developer/i, /coding/i, /codebase/i, /refactor/i, /implementation/i, /repository/i, /git\b/i, /github/i, /typescript/i, /javascript/i, /python/i, /java\b/i, /c#\b/i, /golang|go\b/i, /rust\b/i] },
  { category: 'frontend', weight: 2, terms: [/frontend/i, /react/i, /next\.js|nextjs/i, /vue/i, /angular/i, /css\b/i, /html\b/i, /web component/i] },
  { category: 'backend-api', weight: 2, terms: [/backend/i, /api\b/i, /rest\b/i, /graphql/i, /grpc/i, /server/i, /endpoint/i, /fastapi/i, /express/i, /django/i, /spring/i] },
  { category: 'database-data', weight: 2, terms: [/database/i, /sql\b/i, /postgres/i, /mysql/i, /sqlite/i, /mongodb/i, /redis/i, /migration/i, /schema/i, /etl\b/i, /data pipeline/i] },
  { category: 'ai-ml', weight: 2, terms: [/machine learning/i, /\bml\b/i, /llm/i, /agent/i, /rag\b/i, /embedding/i, /prompt/i, /model eval/i, /fine[- ]tun/i] },
  { category: 'testing-qa', weight: 3, terms: [/test/i, /qa\b/i, /quality assurance/i, /unit test/i, /integration test/i, /e2e/i, /end[- ]to[- ]end/i, /playwright/i, /cypress/i, /jest/i, /vitest/i, /coverage/i, /mutation test/i] },
  { category: 'security', weight: 3, terms: [/security/i, /vulnerab/i, /owasp/i, /sast/i, /dast/i, /threat model/i, /secret scan/i, /dependency scan/i, /authentication/i, /authorization/i, /oauth/i, /oidc/i, /zero trust/i] },
  { category: 'code-quality-review', weight: 3, terms: [/code review/i, /lint/i, /static analysis/i, /code quality/i, /sonarqube/i, /complexity/i, /technical debt/i, /pull request/i, /merge request/i] },
  { category: 'ci-cd-automation', weight: 3, terms: [/ci\/cd/i, /continuous integration/i, /continuous delivery/i, /github actions/i, /gitlab ci/i, /jenkins/i, /pipeline/i, /automation/i, /workflow/i] },
  { category: 'cloud-infrastructure', weight: 3, terms: [/cloud/i, /aws\b/i, /azure/i, /gcp\b/i, /terraform/i, /pulumi/i, /kubernetes/i, /k8s/i, /docker/i, /container/i, /infrastructure as code/i, /iac\b/i] },
  { category: 'deployment-release', weight: 3, terms: [/deploy/i, /release/i, /production/i, /staging/i, /rollout/i, /rollback/i, /blue[- ]green/i, /canary/i, /feature flag/i, /vercel/i, /netlify/i] },
  { category: 'observability-operations', weight: 3, terms: [/observability/i, /monitor/i, /logging/i, /metrics/i, /tracing/i, /sentry/i, /datadog/i, /prometheus/i, /grafana/i, /incident/i, /on[- ]call/i, /sre\b/i] },
  { category: 'maintenance-debugging', weight: 3, terms: [/maintenan/i, /debug/i, /troubleshoot/i, /bug fix/i, /root cause/i, /upgrade/i, /dependency update/i, /performance/i, /profil/i, /optimization/i] },
  { category: 'documentation-knowledge', weight: 2, terms: [/documentation/i, /readme/i, /changelog/i, /runbook/i, /knowledge base/i, /adr\b/i, /api docs/i, /technical writing/i] },
  { category: 'collaboration-governance', weight: 2, terms: [/governance/i, /compliance/i, /policy/i, /collaboration/i, /handoff/i, /stakeholder/i, /raci/i, /change management/i, /approval/i] },
];

const STRONG_OUT_OF_SCOPE = [
  /horoscope|astrology|zodiac/i,
  /recipe|cooking|baking|cocktail/i,
  /dating|relationship advice/i,
  /workout|fitness plan|nutrition/i,
  /travel itinerary|tourism/i,
  /gaming cheat|game walkthrough/i,
  /poetry|fiction writing|songwriting/i,
];

export function classifySkill(input: { id: string; name?: string | null; source?: string | null; skillMd: string }): SkillClassification {
  const text = `${input.id}\n${input.name ?? ''}\n${input.source ?? ''}\n${input.skillMd}`.slice(0, 200_000);
  const categories = new Set<LifecycleCategory>();
  const matchedSignals: string[] = [];
  let score = 0;

  for (const rule of RULES) {
    const matched = rule.terms.filter((term) => term.test(text));
    if (matched.length > 0) {
      categories.add(rule.category);
      score += rule.weight + Math.min(2, matched.length - 1);
      matchedSignals.push(`${rule.category}:${matched.length}`);
    }
  }

  const negativeHits = STRONG_OUT_OF_SCOPE.filter((term) => term.test(text)).length;
  if (negativeHits > 0 && categories.size === 0) score -= negativeHits * 3;

  let decision: ClassificationDecision;
  if (categories.size >= 1 && score >= 3) decision = 'KEEP';
  else if (categories.size >= 1 || score >= 1) decision = 'REVIEW';
  else decision = 'OUT_OF_SCOPE';

  return {
    decision,
    score,
    categories: [...categories],
    matchedSignals,
    reason:
      decision === 'KEEP'
        ? `Direct lifecycle relevance across ${categories.size} categor${categories.size === 1 ? 'y' : 'ies'}.`
        : decision === 'REVIEW'
          ? 'Potentially useful to the software/project lifecycle; retained for later review to avoid false negatives.'
          : 'No meaningful software/project lifecycle signal detected by the conservative classifier.',
  };
}
