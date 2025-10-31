import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    coverage: {
      enabled: true,
      reportsDirectory: './coverage',
      reporter: ['text', 'lcov'],
      include: ['src/*.ts', 'src/**/*.ts'],
      exclude: ['src/interfaces', '**/node_modules/**']
    },
    include: ['tests/*.ts']
  }
});
