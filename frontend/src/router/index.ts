import { createRouter, createWebHistory } from 'vue-router'
import { showSettings } from '@/store/music'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: '/',
            redirect: '/add'
        },
        {
            path: '/add',
            name: 'add',
            component: () => import('../views/AddMusic.vue')
        },
        {
            path: '/compare',
            name: 'compare',
            component: () => import('../views/Compare.vue')
        },
        {
            path: '/settings',
            name: 'settings',
            component: () => import('../views/Settings.vue'),
            beforeEnter: () => {
                if (!showSettings.value) {
                    return { path: '/add' }
                }
            }
        }
    ]
})

export default router
